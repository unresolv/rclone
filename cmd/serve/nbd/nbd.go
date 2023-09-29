// Package nbd provides a network block device server
package nbd

import (
	"bufio"
	"context"
	_ "embed"
	"errors"
	"fmt"
	"io"
	"log"
	"math/bits"
	"path/filepath"
	"strings"
	"sync"

	"github.com/rclone/gonbdserver/nbd"
	"github.com/rclone/rclone/cmd"
	"github.com/rclone/rclone/cmd/serve/proxy/proxyflags"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/flags"
	"github.com/rclone/rclone/fs/rc"
	"github.com/rclone/rclone/lib/systemd"
	"github.com/rclone/rclone/vfs"
	"github.com/rclone/rclone/vfs/vfscommon"
	"github.com/rclone/rclone/vfs/vfsflags"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

const logPrefix = "nbd"

// Options required for nbd server
type Options struct {
	ListenAddr         string // Port to listen on
	ReadOnly           string // Set for read only
	MinBlockSize       fs.SizeSuffix
	PreferredBlockSize fs.SizeSuffix
	MaxBlockSize       fs.SizeSuffix
	Create             fs.SizeSuffix
	ChunkSize          fs.SizeSuffix
	Log2ChunkSize      uint
	Resize             fs.SizeSuffix
	// name := flag.String("name", "default", "Export name")
	// description := flag.String("description", "The default export", "Export description")
}

// DefaultOpt is the default values used for Options
var DefaultOpt = Options{
	ListenAddr:         "localhost:10809",
	MinBlockSize:       512,         // FIXME
	PreferredBlockSize: 4096,        // this is the max according to nbd-client
	MaxBlockSize:       1024 * 1024, // FIXME
}

// Opt is options set by command line flags
var Opt = DefaultOpt

// AddFlags adds flags for the nbd
func AddFlags(flagSet *pflag.FlagSet, Opt *Options) {
	rc.AddOption("nbd", &Opt)
	flags.StringVarP(flagSet, &Opt.ListenAddr, "addr", "", Opt.ListenAddr, "IPaddress:Port or :Port to bind server to", "")
	flags.FVarP(flagSet, &Opt.MinBlockSize, "min-block-size", "", "Minimum block size to advertise", "")
	flags.FVarP(flagSet, &Opt.PreferredBlockSize, "preferred-block-size", "", "Preferred block size to advertise", "")
	flags.FVarP(flagSet, &Opt.MaxBlockSize, "max-block-size", "", "Maximum block size to advertise", "")
	flags.FVarP(flagSet, &Opt.Create, "create", "", "If the destination does not exist, create it with this size", "")
	flags.FVarP(flagSet, &Opt.ChunkSize, "chunk-size", "", "If creating the destination use this chunk size. Must be a power of 2.", "")
	flags.FVarP(flagSet, &Opt.Resize, "resize", "", "If the destination does, resize it to this size", "")
}

func init() {
	flagSet := Command.Flags()
	vfsflags.AddFlags(flagSet)
	proxyflags.AddFlags(flagSet)
	AddFlags(flagSet, &Opt)
}

//go:embed nbd.md
var helpText string

// Command definition for cobra
var Command = &cobra.Command{
	Use:   "nbd remote:path",
	Short: `Serve the remote over NBD.`,
	Long:  helpText + vfs.Help,
	Annotations: map[string]string{
		"versionIntroduced": "v1.65",
		"status":            "experimental",
	},
	Run: func(command *cobra.Command, args []string) {
		// FIXME could serve more than one nbd?
		cmd.CheckArgs(1, 1, command, args)
		f, leaf := cmd.NewFsFile(args[0])

		cmd.Run(false, true, command, func() error {
			s, err := run(context.Background(), f, leaf, Opt)
			if err != nil {
				log.Fatal(err)
			}

			defer systemd.Notify()()
			// FIXME
			_ = s
			s.Wait()
			return nil
		})
	},
}

// NBD contains everything to run the server
type NBD struct {
	f                fs.Fs
	leaf             string
	vfs              *vfs.VFS // don't use directly, use getVFS
	opt              Options
	wg               sync.WaitGroup
	sessionWaitGroup sync.WaitGroup
	logRd            *io.PipeReader
	logWr            *io.PipeWriter

	backendFactory backendFactory
}

// interface for creating backend factories
type backendFactory interface {
	newBackend(ctx context.Context, ec *nbd.ExportConfig) (nbd.Backend, error)
}

// Create and start the server for nbd either on directory f or using file leaf in f
func run(ctx context.Context, f fs.Fs, leaf string, opt Options) (s *NBD, err error) {
	if opt.ChunkSize != 0 {
		if set := bits.OnesCount64(uint64(opt.ChunkSize)); set != 1 {
			return nil, fmt.Errorf("--chunk-size must be a power of 2 (counted %d bits set)", set)
		}
		opt.Log2ChunkSize = uint(bits.TrailingZeros64(uint64(opt.ChunkSize)))
		fs.Debugf(logPrefix, "Using ChunkSize %v (%v), Log2ChunkSize %d", opt.ChunkSize, fs.SizeSuffix(1<<opt.Log2ChunkSize), opt.Log2ChunkSize)
	}
	if !vfsflags.Opt.ReadOnly && vfsflags.Opt.CacheMode < vfscommon.CacheModeWrites {
		return nil, errors.New("need --vfs-cache-mode writes or full when serving read/write")
	}

	s = &NBD{
		f:    f,
		leaf: leaf,
		opt:  opt,
		vfs:  vfs.New(f, &vfsflags.Opt),
	}

	// Create the backend factory
	if leaf != "" {
		s.backendFactory, err = s.newFileBackendFactory(ctx)
	} else {
		s.backendFactory, err = s.newChunkedBackendFactory(ctx)
	}
	if err != nil {
		return nil, err
	}
	nbd.RegisterBackend("rclone", s.backendFactory.newBackend)
	fs.Debugf(logPrefix, "Registered backends: %v", nbd.GetBackendNames())

	var (
		protocol = "tcp"
		addr     = Opt.ListenAddr
	)
	if strings.HasPrefix(addr, "unix://") || filepath.IsAbs(addr) {
		protocol = "unix"
		addr = strings.TrimPrefix(addr, "unix://")

	}

	ec := nbd.ExportConfig{
		Name:               "default",
		Description:        fs.ConfigString(f),
		Driver:             "rclone",
		ReadOnly:           vfsflags.Opt.ReadOnly,
		Workers:            8,     // should this be --checkers or a new config flag FIXME
		TLSOnly:            false, // FIXME
		MinimumBlockSize:   uint64(Opt.MinBlockSize),
		PreferredBlockSize: uint64(Opt.PreferredBlockSize),
		MaximumBlockSize:   uint64(Opt.MaxBlockSize),
		DriverParameters: nbd.DriverParametersConfig{
			"sync": "false",
			"path": "/tmp/diskimage",
		},
	}

	// Make a logger to feed gonbdserver's logs into rclone's logging system
	s.logRd, s.logWr = io.Pipe()
	go func() {
		scanner := bufio.NewScanner(s.logRd)
		for scanner.Scan() {
			line := scanner.Text()
			if s, ok := strings.CutPrefix(line, "[DEBUG] "); ok {
				fs.Debugf(logPrefix, "%s", s)
			} else if s, ok := strings.CutPrefix(line, "[INFO] "); ok {
				fs.Infof(logPrefix, "%s", s)
			} else if s, ok := strings.CutPrefix(line, "[WARN] "); ok {
				fs.Logf(logPrefix, "%s", s)
			} else if s, ok := strings.CutPrefix(line, "[ERROR] "); ok {
				fs.Errorf(logPrefix, "%s", s)
			} else if s, ok := strings.CutPrefix(line, "[CRIT] "); ok {
				fs.Errorf(logPrefix, "%s", s)
			} else {
				fs.Infof(logPrefix, "%s", line)
			}
		}
		if err := scanner.Err(); err != nil {
			fs.Errorf(logPrefix, "Log writer failed: %v", err)
		}
	}()
	logger := log.New(s.logWr, "", 0)

	ci := fs.GetConfig(ctx)
	dump := ci.Dump & (fs.DumpHeaders | fs.DumpBodies | fs.DumpAuth | fs.DumpRequests | fs.DumpResponses)
	var serverConfig = nbd.ServerConfig{
		Protocol:      protocol,               // protocol it should listen on (in net.Conn form)
		Address:       addr,                   // address to listen on
		DefaultExport: "default",              // name of default export
		Exports:       []nbd.ExportConfig{ec}, // array of configurations of exported items
		//TLS:             nbd.TLSConfig{},        // TLS configuration
		DisableNoZeroes: false,     // Disable NoZereos extension FIXME
		Debug:           dump != 0, // Verbose debug
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		// FIXME contexts
		nbd.StartServer(ctx, ctx, &s.sessionWaitGroup, logger, serverConfig)
	}()

	return s, nil
}

// Wait for the server to finish
func (s *NBD) Wait() {
	s.wg.Wait()
	_ = s.logWr.Close()
	_ = s.logRd.Close()
}
