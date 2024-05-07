package main

import (
	"encoding/json"
	"errors"
	"log"
	"os"

	"github.com/ipfs/boxo/ipns"
	"github.com/ipfs/boxo/routing/http/types"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multibase"
	"github.com/urfave/cli/v2"
)

const cidContactEndpoint = "https://cid.contact"

func main() {
	app := &cli.App{
		Name:    name,
		Usage:   "A Delegated Routing V1 server and proxy for all your routing needs.",
		Version: version,
		Commands: []*cli.Command{
			{
				Name: "start",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:    "listen-address",
						Value:   "127.0.0.1:8190",
						EnvVars: []string{"SOMEGUY_LISTEN_ADDRESS"},
						Usage:   "listen address",
					},
					&cli.BoolFlag{
						Name:    "accelerated-dht",
						Value:   true,
						EnvVars: []string{"SOMEGUY_ACCELERATED_DHT"},
						Usage:   "run the accelerated DHT client",
					},
					&cli.BoolFlag{
						Name:    "put-enabled",
						Value:   false,
						EnvVars: []string{"SOMEGUY_PUT_ENABLED"},
						Usage:   "enables HTTP PUT endpoints",
					},
					&cli.StringFlag{
						Name:    "datadir",
						Value:   "",
						EnvVars: []string{"SOMEGUY_DATADIR"},
						Usage:   "directory for persistent data",
					},
					&cli.StringSliceFlag{
						Name:    "provider-endpoints",
						Value:   cli.NewStringSlice(cidContactEndpoint),
						EnvVars: []string{"SOMEGUY_PROVIDER_ENDPOINTS"},
						Usage:   "other Delegated Routing V1 endpoints to proxy provider requests to",
					},
					&cli.StringSliceFlag{
						Name:    "peer-endpoints",
						Value:   cli.NewStringSlice(),
						EnvVars: []string{"SOMEGUY_PEER_ENDPOINTS"},
						Usage:   "other Delegated Routing V1 endpoints to proxy peer requests to",
					},
					&cli.StringSliceFlag{
						Name:    "ipns-endpoints",
						Value:   cli.NewStringSlice(),
						EnvVars: []string{"SOMEGUY_IPNS_ENDPOINTS"},
						Usage:   "other Delegated Routing V1 endpoints to proxy IPNS requests to",
					},
				},
				Action: func(ctx *cli.Context) error {
					options := &serverOptions{
						listenAddress:    ctx.String("listen-address"),
						acceleratedDHT:   ctx.Bool("accelerated-dht"),
						putEnabled:       ctx.Bool("put-enabled"),
						contentEndpoints: ctx.StringSlice("provider-endpoints"),
						peerEndpoints:    ctx.StringSlice("peer-endpoints"),
						ipnsEndpoints:    ctx.StringSlice("ipns-endpoints"),
						dataDirectory:    ctx.String("datadir"),
					}

					return startServer(ctx.Context, options)
				},
			},
			{
				Name: "ask",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:  "endpoint",
						Value: cidContactEndpoint,
						Usage: "the Delegated Routing V1 endpoint to ask",
					},
					&cli.BoolFlag{
						Name:  "pretty",
						Value: false,
						Usage: "output data in a prettier format that may convey less information",
					},
				},
				Subcommands: []*cli.Command{
					{
						Name:      "findprovs",
						Usage:     "findprovs <cid>",
						UsageText: "Find providers of a given CID",
						Action: func(ctx *cli.Context) error {
							if ctx.NArg() != 1 {
								return errors.New("invalid command, see help")
							}
							cidStr := ctx.Args().Get(0)
							c, err := cid.Parse(cidStr)
							if err != nil {
								return err
							}
							cl, err := newAskClient(ctx.String("endpoint"), ctx.Bool("pretty"), os.Stdout)
							if err != nil {
								return err
							}
							return cl.findProviders(ctx.Context, c)
						},
					},
					{
						Name:      "provide",
						Usage:     "provide <record-path...>",
						UsageText: "Provide a one or multiple announcement records to the network",
						Action: func(ctx *cli.Context) error {
							if ctx.NArg() < 1 {
								return errors.New("invalid command, see help")
							}
							records := []*types.AnnouncementRecord{}
							for _, recordPath := range ctx.Args().Slice() {
								recordData, err := os.ReadFile(recordPath)
								if err != nil {
									return err
								}
								var record *types.AnnouncementRecord
								err = json.Unmarshal(recordData, &record)
								if err != nil {
									return err
								}
								records = append(records, record)
							}
							cl, err := newAskClient(ctx.String("endpoint"), ctx.Bool("pretty"), os.Stdout)
							if err != nil {
								return err
							}
							return cl.provide(ctx.Context, records...)
						},
					},
					{
						Name:      "findpeers",
						Usage:     "findpeers <pid>",
						UsageText: "Find a peer of a given PID",
						Action: func(ctx *cli.Context) error {
							if ctx.NArg() != 1 {
								return errors.New("invalid command, see help")
							}
							pidStr := ctx.Args().Get(0)
							pid, err := peer.Decode(pidStr)
							if err != nil {
								return err
							}
							cl, err := newAskClient(ctx.String("endpoint"), ctx.Bool("pretty"), os.Stdout)
							if err != nil {
								return err
							}
							return cl.findPeers(ctx.Context, pid)
						},
					},
					{
						Name:      "providepeers",
						Usage:     "providepeers <record-path...>",
						UsageText: "Provide a one or multiple peer announcement records to the network",
						Action: func(ctx *cli.Context) error {
							if ctx.NArg() < 1 {
								return errors.New("invalid command, see help")
							}
							records := []*types.AnnouncementRecord{}
							for _, recordPath := range ctx.Args().Slice() {
								recordData, err := os.ReadFile(recordPath)
								if err != nil {
									return err
								}
								var record *types.AnnouncementRecord
								err = json.Unmarshal(recordData, &record)
								if err != nil {
									return err
								}
								records = append(records, record)
							}
							cl, err := newAskClient(ctx.String("endpoint"), ctx.Bool("pretty"), os.Stdout)
							if err != nil {
								return err
							}
							return cl.providePeer(ctx.Context, records...)
						},
					},
					{
						Name:      "getipns",
						Usage:     "getipns <ipns-id>",
						UsageText: "Get the value of an IPNS ID",
						Action: func(ctx *cli.Context) error {
							if ctx.NArg() != 1 {
								return errors.New("invalid command, see help")
							}
							nameStr := ctx.Args().Get(0)
							name, err := ipns.NameFromString(nameStr)
							if err != nil {
								return err
							}
							cl, err := newAskClient(ctx.String("endpoint"), ctx.Bool("pretty"), os.Stdout)
							if err != nil {
								return err
							}
							return cl.getIPNS(ctx.Context, name)
						},
					},
					{
						Name:      "putipns",
						Usage:     "putipns <ipns-id> <multibase-encoded-record>",
						UsageText: "Put an IPNS record",
						Flags:     []cli.Flag{},
						Action: func(ctx *cli.Context) error {
							if ctx.NArg() != 2 {
								return errors.New("invalid command, see help")
							}
							nameStr := ctx.Args().Get(0)
							name, err := ipns.NameFromString(nameStr)
							if err != nil {
								return err
							}
							recordStr := ctx.Args().Get(1)
							_, recBytes, err := multibase.Decode(recordStr)
							if err != nil {
								return err
							}
							cl, err := newAskClient(ctx.String("endpoint"), ctx.Bool("pretty"), os.Stdout)
							if err != nil {
								return err
							}
							return cl.putIPNS(ctx.Context, name, recBytes)
						},
					},
				},
			},
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
