package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/ipfs/boxo/ipns"
	"github.com/ipfs/boxo/routing/http/client"
	"github.com/ipfs/boxo/routing/http/types"
	"github.com/ipfs/boxo/routing/http/types/iter"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
)

type askClient struct {
	drc    *client.Client
	out    io.Writer
	pretty bool
}

func newAskClient(endpoint string, prettyOutput bool, out io.Writer) (*askClient, error) {
	drc, err := client.New(endpoint)
	if err != nil {
		return nil, err
	}

	if out == nil {
		out = os.Stdout
	}

	return &askClient{
		drc:    drc,
		pretty: prettyOutput,
		out:    out,
	}, nil
}

func (a *askClient) findProviders(ctx context.Context, key cid.Cid) error {
	recordsIter, err := a.drc.FindProviders(ctx, key)
	if err != nil {
		return err
	}
	defer recordsIter.Close()

	for recordsIter.Next() {
		res := recordsIter.Val()

		// Check for error, but do not complain if we exceeded the timeout. We are
		// expecting that to happen: we explicitly defined a timeout.
		if res.Err != nil {
			if !errors.Is(res.Err, context.DeadlineExceeded) {
				return res.Err
			}

			return nil
		}

		if a.pretty {
			switch res.Val.GetSchema() {
			case types.SchemaPeer:
				record := res.Val.(*types.PeerRecord)
				fmt.Fprintln(a.out, record.ID)
				fmt.Fprintln(a.out, "\tProtocols:", record.Protocols)
				fmt.Fprintln(a.out, "\tAddresses:", record.Addrs)
			default:
				// This is an unknown schema. Let's just print it raw.
				err := json.NewEncoder(a.out).Encode(res.Val)
				if err != nil {
					return err
				}
			}

			fmt.Fprintln(a.out)
		} else {
			err := json.NewEncoder(a.out).Encode(res.Val)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (a *askClient) provide(ctx context.Context, records ...*types.AnnouncementRecord) error {
	for _, rec := range records {
		err := rec.Verify()
		if err != nil {
			return err
		}
	}

	recordsIter, err := a.drc.ProvideRecords(ctx, records...)
	if err != nil {
		return err
	}
	defer recordsIter.Close()
	return a.printProvideResult(recordsIter)
}

func (a *askClient) printProvideResult(recordsIter iter.ResultIter[*types.AnnouncementResponseRecord]) error {
	for recordsIter.Next() {
		res := recordsIter.Val()

		// Check for error, but do not complain if we exceeded the timeout. We are
		// expecting that to happen: we explicitly defined a timeout.
		if res.Err != nil {
			if !errors.Is(res.Err, context.DeadlineExceeded) {
				return res.Err
			}

			return nil
		}

		if a.pretty {
			if res.Val.Error != "" {
				fmt.Fprintf(a.out, "Error: %s", res.Val.Error)
			} else {
				fmt.Fprintf(a.out, "TTL: %s", res.Val.TTL)
			}
			fmt.Fprintln(a.out)
		} else {
			err := json.NewEncoder(a.out).Encode(res.Val)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (a *askClient) findPeers(ctx context.Context, pid peer.ID) error {
	recordsIter, err := a.drc.FindPeers(ctx, pid)
	if err != nil {
		return err
	}
	defer recordsIter.Close()

	for recordsIter.Next() {
		res := recordsIter.Val()

		// Check for error, but do not complain if we exceeded the timeout. We are
		// expecting that to happen: we explicitly defined a timeout.
		if res.Err != nil {
			if !errors.Is(res.Err, context.DeadlineExceeded) {
				return res.Err
			}

			return nil
		}

		if a.pretty {
			fmt.Fprintln(a.out, res.Val.ID)
			fmt.Fprintln(a.out, "\tProtocols:", res.Val.Protocols)
			fmt.Fprintln(a.out, "\tAddresses:", res.Val.Addrs)
			fmt.Fprintln(a.out)
		} else {
			err := json.NewEncoder(a.out).Encode(res.Val)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (a *askClient) providePeer(ctx context.Context, records ...*types.AnnouncementRecord) error {
	for _, rec := range records {
		err := rec.Verify()
		if err != nil {
			return err
		}
	}

	recordsIter, err := a.drc.ProvidePeerRecords(ctx, records...)
	if err != nil {
		return err
	}
	defer recordsIter.Close()

	return a.printProvideResult(recordsIter)
}

func (a *askClient) getIPNS(ctx context.Context, name ipns.Name) error {
	rec, err := a.drc.GetIPNS(ctx, name)
	if err != nil {
		return err
	}

	if a.pretty {
		v, err := rec.Value()
		if err != nil {
			return err
		}

		seq, err := rec.Sequence()
		if err != nil {
			return err
		}

		eol, err := rec.Validity()
		if err != nil {
			return err
		}

		fmt.Fprintf(a.out, "/ipns/%s\n", name)

		// Since [client.Client.GetIPNS] verifies if the retrieved record is valid, we
		// do not need to verify it again. However, if you were not using this specific
		// client, but using some other tool, you should always validate the IPNS Record
		// using the [ipns.Validate] or [ipns.ValidateWithName] functions.
		fmt.Fprintln(a.out, "\tSignature Validated")
		fmt.Fprintln(a.out, "\tValue:", v.String())
		fmt.Fprintln(a.out, "\tSequence:", seq)
		fmt.Fprintln(a.out, "\tValidityType : EOL/End-of-Life")
		fmt.Fprintln(a.out, "\tValidity:", eol.Format(time.RFC3339))
		if ttl, err := rec.TTL(); err == nil {
			fmt.Fprintln(a.out, "\tTTL:", ttl.String())
		}

		return nil
	}

	raw, err := ipns.MarshalRecord(rec)
	if err != nil {
		return err
	}

	_, err = a.out.Write(raw)
	return err
}

func (a *askClient) putIPNS(ctx context.Context, name ipns.Name, record []byte) error {
	rec, err := ipns.UnmarshalRecord(record)
	if err != nil {
		return err
	}

	return a.drc.PutIPNS(ctx, name, rec)
}
