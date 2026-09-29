package raftlog

import (
	"fmt"
	"os"
	"path/filepath"
)

// HardState is the Raft state that must be durable BEFORE the RPC that
// depends on it is sent: currentTerm and votedFor.
//
// Persisting these before responding to RequestVote (and before incrementing
// term) is what prevents a crashed node from voting twice in the same term.
type HardState struct {
	Term     uint64
	VotedFor string // node ID; "" = no vote in this term
}

// maxVotedForLen bounds the node-ID field so a corrupt len cannot cause a
// huge allocation.
const maxVotedForLen = 1024

// SetHardState durably persists hs.
//
// Durability policy: temp file → fsync → atomic rename → fsync directory.
// Rename atomicity means the on-disk hard state is always either the previous
// value or the new one — never a partially-written mixture. Raft calls this
// and relies on it completing before sending the RPC that assumes the new
// term/vote.
func (l *Log) SetHardState(hs HardState) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	if len(hs.VotedFor) > maxVotedForLen {
		return fmt.Errorf("raftlog: VotedFor exceeds %d bytes", maxVotedForLen)
	}

	// Encode: [len u32][crc u32][term u64][vlen u32][votedFor]
	body := 8 + 4 + len(hs.VotedFor)
	buf := make([]byte, headerSize+body)
	putU32(buf[0:4], uint32(body))
	putU64(buf[8:16], hs.Term)
	putU32(buf[16:20], uint32(len(hs.VotedFor)))
	copy(buf[20:], hs.VotedFor)
	// CRC covers len field + body (everything except the crc field itself).
	putU32(buf[4:8], crcOver(buf[0:4], buf[8:]))

	tmp := filepath.Join(l.dir, "hardstate.tmp")
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("raftlog: hard state temp: %w", err)
	}
	if err := writeFull(f, buf); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("raftlog: hard state write: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("raftlog: hard state sync: %w", err)
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("raftlog: hard state close: %w", err)
	}
	if err := os.Rename(tmp, hardPath(l.dir)); err != nil {
		return fmt.Errorf("raftlog: hard state rename: %w", err)
	}
	if err := syncDir(l.dir); err != nil {
		return fmt.Errorf("raftlog: hard state sync dir: %w", err)
	}

	l.hard = hs
	return nil
}

// readHardState loads the hard-state file. A missing file means a fresh node
// (term 0, no vote). An existing-but-unparseable file is ErrHardStateCorrupt:
// zeroing it silently could permit a double-vote, so recovery refuses.
func readHardState(dir string) (HardState, error) {
	data, err := os.ReadFile(hardPath(dir))
	if err != nil {
		if os.IsNotExist(err) {
			return HardState{}, nil
		}
		return HardState{}, fmt.Errorf("raftlog: read hard state: %w", err)
	}
	if len(data) == 0 {
		// Zero-length file: crash before any content landed (rename atomicity
		// makes this a "never written" state, equivalent to missing).
		return HardState{}, nil
	}
	if len(data) < headerSize {
		return HardState{}, fmt.Errorf("%w: truncated header (%d bytes)", ErrHardStateCorrupt, len(data))
	}

	body := int(getU32(data[0:4]))
	end := headerSize + body
	if body < 12 || end != len(data) {
		return HardState{}, fmt.Errorf("%w: bad length %d (file %d bytes)", ErrHardStateCorrupt, body, len(data))
	}
	if getU32(data[4:8]) != crcOver(data[0:4], data[8:]) {
		return HardState{}, fmt.Errorf("%w: CRC mismatch", ErrHardStateCorrupt)
	}

	vlen := int(getU32(data[16:20]))
	if vlen > maxVotedForLen || 8+4+vlen != body {
		return HardState{}, fmt.Errorf("%w: bad VotedFor length %d", ErrHardStateCorrupt, vlen)
	}
	return HardState{
		Term:     getU64(data[8:16]),
		VotedFor: string(data[20 : 20+vlen]),
	}, nil
}
