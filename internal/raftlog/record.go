package raftlog

import (
	"encoding/binary"
	"hash/crc32"
)

// On-disk record format (little-endian):
//
//	0        4          8                24          24+N
//	+--------+----------+----------------+------------+
//	| len u32| crc32 u32| index u64      | term u64   | payload (N bytes)
//	+--------+----------+----------------+------------+
//
//	len  = 16 + N  (size of index+term+payload, i.e. the body after the
//	              8-byte header)
//	crc  = CRC-32 (IEEE) over  len bytes || body bytes
//	      — the len field itself is covered, so corruption of len is
//	      detected rather than causing a mis-parse.
//
// The payload is opaque to this package: from Phase 7 on it holds the
// encoded Raft command.
const (
	headerSize    = 8  // len(4) + crc(4)
	entryMetaSize = 16 // index(8) + term(8)

	// maxBodySize bounds the len field. A len larger than this cannot be a
	// record we ever wrote (Append rejects oversized payloads), so it marks
	// garbage bytes — parseRecords treats it exactly like a torn tail or
	// corruption depending on where it sits.
	maxBodySize = 16 << 20 // 16 MiB of index+term+payload per record
)

var ieee = crc32.MakeTable(crc32.IEEE)

// crcOver computes CRC-32 over two byte ranges (len field + body).
func crcOver(a, b []byte) uint32 {
	c := crc32.Update(0, ieee, a)
	return crc32.Update(c, ieee, b)
}

// recordLen returns the total encoded size of a record with payload n.
func recordLen(n int) int { return headerSize + entryMetaSize + n }

// appendRecord encodes e onto dst and returns the extended slice.
func appendRecord(dst []byte, e Entry) []byte {
	body := entryMetaSize + len(e.Payload)
	if body > maxBodySize {
		// Append validates before calling, but keep the invariant local.
		panic("raftlog: payload too large")
	}
	start := len(dst)
	dst = append(dst, make([]byte, recordLen(len(e.Payload)))...)
	rec := dst[start:]

	binary.LittleEndian.PutUint32(rec[0:4], uint32(body))
	binary.LittleEndian.PutUint64(rec[8:16], e.Index)
	binary.LittleEndian.PutUint64(rec[16:24], e.Term)
	copy(rec[24:], e.Payload)

	// CRC covers len field + everything after the crc field.
	c := crc32.Update(0, ieee, rec[0:4])
	c = crc32.Update(c, ieee, rec[8:])
	binary.LittleEndian.PutUint32(rec[4:8], c)
	return dst
}

// parseRecords decodes every complete, valid record in data.
//
// Return contract:
//   - entries: every record that parsed and verified, in order, with
//     contiguous indexes.
//   - torn: byte offset of an incomplete/invalid FINAL record (>= 0), or -1
//     when data ends on a clean record boundary. The caller truncates the
//     file at that offset.
//   - err: ErrCorrupt when damage exists that cannot be a torn tail — an
//     invalid record followed by more bytes (mid-file corruption), or a
//     discontinuity in indexes.
//
// Why the final record is treated as torn rather than corrupt: Append+Sync
// complete before Raft ever acknowledges an entry, so an invalid last record
// is one whose write never finished — it was never acked. Every other record
// was acked, so damage there is real corruption and must stop recovery.
func parseRecords(data []byte) (entries []Entry, torn int64, err error) {
	off := 0
	torn = -1
	var prevIndex uint64
	havePrev := false

	for off < len(data) {
		// Fewer bytes than a header left: incomplete tail.
		if len(data)-off < headerSize {
			return entries, int64(off), nil
		}
		body := int(binary.LittleEndian.Uint32(data[off : off+4]))

		// A complete header whose len is outside [16, maxBodySize] cannot
		// have been produced by appendRecord: torn writes are always
		// prefixes of a record (partial header < 8 bytes is caught above;
		// a complete header always carries a valid len). So this is real
		// corruption, wherever it sits.
		if body < entryMetaSize || body > maxBodySize {
			return nil, 0, ErrCorrupt
		}

		end := off + headerSize + body

		// Declared record runs past EOF: incomplete final write → torn tail.
		if end > len(data) {
			return entries, int64(off), nil
		}

		// Verify CRC over len field + body.
		c := crcOver(data[off:off+4], data[off+headerSize:end])
		if c != getU32(data[off+4:off+8]) {
			if end == len(data) {
				// Invalid final record = incomplete write.
				return entries, int64(off), nil
			}
			return nil, 0, ErrCorrupt
		}

		index := binary.LittleEndian.Uint64(data[off+8 : off+16])
		term := binary.LittleEndian.Uint64(data[off+16 : off+24])
		if havePrev && index != prevIndex+1 {
			return nil, 0, ErrCorrupt
		}

		payload := data[off+24 : end]
		entries = append(entries, Entry{
			Index:   index,
			Term:    term,
			Payload: payload, // aliases data; Get returns clones
		})
		prevIndex, havePrev = index, true
		off = end
	}
	return entries, -1, nil
}
