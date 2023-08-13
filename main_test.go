package miniwal

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLogFile(t *testing.T) {
	path := "./wal-log-test"
	_ = os.Remove(path)
	defer os.Remove(path)
	entries := [][]byte{[]byte("test"), []byte("foobar")}
	{
		log, err := Open(path)
		require.NoError(t, err)
		for _, entry := range entries {
			require.NoError(t, log.Write(entry))
		}
		require.NoError(t, log.Flush())
		iter, err := log.Iter(log.FirstIndex(), log.LastIndex())
		require.NoError(t, err)
		for _, written := range entries {
			read, err := iter.Next()
			require.NoError(t, err)
			require.Equal(t, written, read)
		}
	}
	{
		log, err := Open(path)
		require.NoError(t, err)
		iter, err := log.Iter(log.FirstIndex(), log.LastIndex())
		require.NoError(t, err)
		var readEntries [][]byte
		for {
			entry, err := iter.Next()
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			readEntries = append(readEntries, entry)
		}
		require.Equal(t, entries, readEntries)
	}
	{
		log, err := Open(path)
		require.NoError(t, err)
		entry, err := log.Seek(log.FirstIndex() + 1)
		require.NoError(t, err)
		var buf bytes.Buffer
		nextEntry, err := entry.ReadToNext(&buf)
		require.NoError(t, err)
		require.Nil(t, nextEntry)
		require.Equal(t, entries[1], buf.Bytes())
	}
	{
		log, err := Open(path)
		require.NoError(t, err)
		entry, err := log.Seek(log.FirstIndex() + 1)
		require.NoError(t, err)
		_, err = entry.Seek(log.FirstIndex())
		require.Error(t, err)
	}
}

func TestLogFileCompaction(t *testing.T) {
	path := "./wal-log-compaction"
	_ = os.Remove(path)
	defer os.Remove(path)
	entries := [][]byte{
		[]byte("test"),
		[]byte("foobar"),
		[]byte("bbb"),
		[]byte("aaaaa"),
		[]byte("11"),
		[]byte("222"),
		bytes.Repeat([]byte{9}, 200),
		[]byte("bar"),
	}
	{
		log, err := Open(path)
		require.NoError(t, err)
		for _, entry := range entries {
			require.NoError(t, log.Write(entry))
		}
		require.Equal(t, uint64(0), log.FirstIndex())
		require.NoError(t, log.Compact(4))
		require.Equal(t, uint64(4), log.FirstIndex())
		iter, err := log.Iter(log.FirstIndex(), log.LastIndex())
		require.NoError(t, err)
		var readEntries [][]byte
		for {
			entry, err := iter.Next()
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			readEntries = append(readEntries, entry)
		}
		require.Equal(t, entries[4:], readEntries)
		require.NoError(t, log.Flush())
	}
	{
		log, err := Open(path)
		require.NoError(t, err)
		require.Equal(t, uint64(4), log.FirstIndex())
		iter, err := log.Iter(log.FirstIndex(), log.LastIndex())
		require.NoError(t, err)
		var readEntries [][]byte
		for {
			entry, err := iter.Next()
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			readEntries = append(readEntries, entry)
		}
		require.Equal(t, entries[4:], readEntries)
	}
}

func TestLogFileRestart(t *testing.T) {
	path := "./wal-log-restart"
	_ = os.Remove(path)
	defer os.Remove(path)
	entries := [][]byte{
		[]byte("test"),
		[]byte("foobar"),
		[]byte("bbb"),
		[]byte("aaaaa"),
		[]byte("11"),
		[]byte("222"),
		bytes.Repeat([]byte{9}, 200),
		[]byte("bar"),
	}
	{
		log, err := Open(path)
		require.NoError(t, err)
		for _, entry := range entries {
			require.NoError(t, log.Write(entry))
		}
		require.Equal(t, uint64(0), log.FirstIndex())
		require.NoError(t, log.Flush())
	}
	{
		log, err := Open(path)
		require.NoError(t, err)
		require.NoError(t, log.Restart(3))
		require.Equal(t, uint64(3), log.FirstIndex())
		iter, err := log.Iter(log.FirstIndex(), log.LastIndex())
		require.NoError(t, err)
		_, err = iter.Next()
		require.EqualError(t, err, io.EOF.Error())
	}
	{
		log, err := Open(path)
		require.NoError(t, err)
		require.Equal(t, uint64(3), log.FirstIndex())
		iter, err := log.Iter(log.FirstIndex(), log.LastIndex())
		require.NoError(t, err)
		_, err = iter.Next()
		require.EqualError(t, err, io.EOF.Error())
	}
}

func TestLogFileHandlesTrimmedWAL(t *testing.T) {
	path := "./wal-log-test-trimmed"
	_ = os.Remove(path)
	defer os.Remove(path)
	entries := [][]byte{[]byte("test"), []byte("foobar")}
	{
		log, err := Open(path)
		require.NoError(t, err)
		for _, entry := range entries {
			require.NoError(t, log.Write(entry))
		}
		require.NoError(t, log.Flush())
	}
	{
		file, err := os.OpenFile(path, os.O_WRONLY, os.ModePerm)
		require.NoError(t, err)
		require.NoError(t, file.Truncate(38))
		require.NoError(t, file.Close())
	}
	{
		log, err := Open(path)
		require.NoError(t, err)
		iter, err := log.Iter(log.FirstIndex(), log.LastIndex())
		require.NoError(t, err)
		var readEntries [][]byte
		for {
			entry, err := iter.Next()
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			readEntries = append(readEntries, entry)
		}
		require.Equal(t, entries[:1], readEntries)
	}
}
