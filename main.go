package miniwal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"syscall"
	"unsafe"
)

type LogFile struct {
	file       *os.File
	path       string
	firstIndex uint64
	len        uint64
}

func Open(path string) (*LogFile, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	fileSize, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}
	var entries uint64
	var firstIndex uint64
	if fileSize >= 8 {
		if _, err := file.Seek(0, io.SeekStart); err != nil {
			return nil, err
		}
		if err := binary.Read(file, binary.LittleEndian, &firstIndex); err != nil {
			return nil, err
		}
		pos := int64(8)
		for fileSize-pos > 8 {
			var entryDataLen uint64
			if err := binary.Read(file, binary.LittleEndian, &entryDataLen); err != nil {
				return nil, err
			}
			entryDataLen += 4 // 4 byte checksum
			if fileSize-pos-8 < int64(entryDataLen) {
				break // the entry was not fully written
			}
			entries++
			pos, err = file.Seek(int64(entryDataLen), io.SeekCurrent)
			if err != nil {
				return nil, err
			}
		}
		if err := file.Truncate(pos); err != nil {
			return nil, err
		}
	} else {
		if _, err := file.Write(make([]byte, 8)); err != nil {
			return nil, err
		}
		if err := file.Truncate(8); err != nil {
			return nil, err
		}
	}
	return &LogFile{
		file:       file,
		path:       path,
		firstIndex: firstIndex,
		len:        entries,
	}, nil
}

func (l *LogFile) FirstEntry() (*LogEntry, error) {
	if l.len == 0 {
		return nil, errors.New("out of bounds")
	}
	if _, err := l.file.Seek(8, io.SeekStart); err != nil {
		return nil, err
	}
	return &LogEntry{
		log:   l,
		index: l.firstIndex,
	}, nil
}

func (l *LogFile) Seek(toIndex uint64) (*LogEntry, error) {
	entry, err := l.FirstEntry()
	if err != nil {
		return nil, err
	}
	return entry.Seek(toIndex)
}

func (l *LogFile) FirstIndex() uint64 {
	return l.firstIndex
}

func (l *LogFile) LastIndex() uint64 {
	return l.firstIndex + l.len - 1
}

func (l *LogFile) Iter(start, end uint64) (*LogIterator, error) {
	if l.len == 0 {
		return &LogIterator{
			lastIndex: l.firstIndex,
		}, nil
	}
	lastIndex := end
	if lastIndex > l.LastIndex() {
		lastIndex = l.LastIndex()
	}
	entry, err := l.Seek(start)
	if err != nil {
		return nil, err
	}
	return &LogIterator{
		next:      entry,
		lastIndex: lastIndex,
	}, nil
}

func (l *LogFile) Write(entry []byte) error {
	endPos, err := l.file.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	hash := crc32.ChecksumIEEE(entry)
	result := writeUint64(l.file, uint64(len(entry))) &&
		writeAll(l.file, entry) &&
		writeUint32(l.file, hash)
	if result {
		l.len++
	} else {
		// Trim the data written.
		if err := l.file.Truncate(endPos + 1); err != nil {
			return err
		}
	}
	return nil
}

func (l *LogFile) Flush() error {
	return l.file.Sync()
}

func (l *LogFile) Compact(newStartIndex uint64) error {
	if err := l.Flush(); err != nil {
		return err
	}
	if _, err := l.Seek(newStartIndex); err != nil {
		return err
	}
	tempFilePath := filepath.Join(os.TempDir(), fmt.Sprintf("log-%d", rand.Uint32()))
	newFile, err := os.OpenFile(tempFilePath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
	if err != nil {
		return err
	}
	binary.Write(newFile, binary.LittleEndian, uint64(newStartIndex))
	if _, err := io.Copy(newFile, l.file); err != nil {
		return err
	}
	if _, _, errno := syscall.Syscall(syscall.SYS_FCNTL, newFile.Fd(), syscall.F_SETLKW64,
		uintptr(unsafe.Pointer(&syscall.Flock_t{
			Type:   syscall.F_WRLCK,
			Whence: int16(io.SeekStart),
			Start:  0,
			Len:    0,
			Pid:    int32(os.Getpid()),
		}))); errno != 0 {
		return errno
	}
	if _, _, errno := syscall.Syscall(syscall.SYS_FCNTL, l.file.Fd(), syscall.F_SETLKW64,
		uintptr(unsafe.Pointer(&syscall.Flock_t{
			Type:   syscall.F_UNLCK,
			Whence: int16(io.SeekStart),
			Start:  0,
			Len:    0,
			Pid:    int32(os.Getpid()),
		}))); errno != 0 {
		return errno
	}
	if _, _, errno := syscall.Syscall(syscall.SYS_FCNTL, l.file.Fd(), syscall.F_SETLKW64,
		uintptr(unsafe.Pointer(&syscall.Flock_t{
			Type:   syscall.F_WRLCK,
			Whence: int16(io.SeekStart),
			Start:  0,
			Len:    0,
			Pid:    int32(os.Getpid()),
		}))); errno != 0 {
		return errno
	}
	if _, _, errno := syscall.Syscall(syscall.SYS_FCNTL, newFile.Fd(), syscall.F_SETLKW64,
		uintptr(unsafe.Pointer(&syscall.Flock_t{
			Type:   syscall.F_UNLCK,
			Whence: int16(io.SeekStart),
			Start:  0,
			Len:    0,
			Pid:    int32(os.Getpid()),
		}))); errno != 0 {
		return errno
	}
	if err := os.Rename(tempFilePath, l.path); err != nil {
		return err
	}
	l.file.Close()
	l.file = newFile
	l.len = l.len - (newStartIndex - l.firstIndex)
	l.firstIndex = newStartIndex
	return nil
}

func (l *LogFile) Restart(startingIndex uint64) error {
	if _, err := l.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if err := binary.Write(l.file, binary.LittleEndian, startingIndex); err != nil {
		return err
	}
	if err := l.file.Truncate(8); err != nil {
		return err
	}
	if err := l.Flush(); err != nil {
		return err
	}
	l.firstIndex = startingIndex
	l.len = 0
	return nil
}

type LogError struct {
	err error
}

func (e LogError) Error() string {
	return e.err.Error()
}

func (l *LogFile) Close() error {
	return l.file.Close()
}

type LogEntry struct {
	log   *LogFile
	index uint64
}

func (e *LogEntry) Index() uint64 {
	return e.index
}

func (e *LogEntry) ReadToNext(w io.Writer) (*LogEntry, error) {
	var size uint64
	if err := binary.Read(e.log.file, binary.LittleEndian, &size); err != nil {
		return nil, err
	}
	hasher := crc32.NewIEEE()
	if _, err := io.CopyN(io.MultiWriter(w, hasher), e.log.file, int64(size)); err != nil {
		return nil, err
	}
	var checksum uint32
	if err := binary.Read(e.log.file, binary.LittleEndian, &checksum); err != nil {
		return nil, err
	}
	if checksum != hasher.Sum32() {
		return nil, errors.New("bad checksum")
	}
	nextIndex := e.index + 1
	if e.log.firstIndex+e.log.len > nextIndex {
		return &LogEntry{
			log:   e.log,
			index: nextIndex,
		}, nil
	}
	return nil, nil
}

func (e *LogEntry) Seek(toIndex uint64) (*LogEntry, error) {
	if toIndex > e.log.firstIndex+e.log.len || toIndex < e.index {
		return nil, errors.New("out of bounds")
	}
	for i := e.index; i < toIndex; i++ {
		var size uint64
		if err := binary.Read(e.log.file, binary.LittleEndian, &size); err != nil {
			return nil, err
		}
		if _, err := e.log.file.Seek(int64(size+4), io.SeekCurrent); err != nil {
			return nil, err
		}
	}
	return &LogEntry{
		log:   e.log,
		index: toIndex,
	}, nil
}

type LogIterator struct {
	next      *LogEntry
	lastIndex uint64
}

func (i *LogIterator) Next() ([]byte, error) {
	if i.next == nil {
		return nil, io.EOF
	}
	if i.next.index > i.lastIndex {
		i.next = nil
		return nil, io.EOF
	}
	var buf bytes.Buffer
	next, err := i.next.ReadToNext(&buf)
	if err != nil {
		return nil, err
	}
	i.next = next
	return buf.Bytes(), nil
}

func (i *LogIterator) Entry() *LogEntry {
	return i.next
}

func writeAll(w io.Writer, data []byte) bool {
	n, err := w.Write(data)
	return err == nil && n == len(data)
}

func writeUint32(w io.Writer, x uint32) bool {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], x)
	return writeAll(w, buf[:])
}

func writeUint64(w io.Writer, x uint64) bool {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], x)
	return writeAll(w, buf[:])
}
