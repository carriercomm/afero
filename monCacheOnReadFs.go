package afero

import (
	"log"
	"os"
	"syscall"
	"time"

	"gopkg.in/fsnotify.v1"
)

// If the cache duration is 0, cache time will be unlimited, i.e. once
// a file is in the layer, the base will never be read again for this file.
//
// For cache times greater than 0, the modification time of a file is
// checked. Note that a lot of file system implementations only allow a
// resolution of a second for timestamps... or as the godoc for os.Chtimes()
// states: "The underlying filesystem may truncate or round the values to a
// less precise time unit."
//
// This caching union will forward all write calls also to the base file
// system first. To prevent writing to the base Fs, wrap it in a read-only
// filter - Note: this will also make the overlay read-only, for writing files
// in the overlay, use the overlay Fs directly, not via the union Fs.
//
// The files in cache are monitored for changes with fsnotify, if one write
// event occur in a file cached the cache will be updated. This is only supported
// with base fs that deal direct with the file system. Cache, union, remote or
// else will not work.
type MonCacheOnReadFs struct {
	base      Fs
	layer     Fs
	cacheTime time.Duration
	watcher   *fsnotify.Watcher
}

func NewMonCacheOnReadFs(base Fs, layer Fs, cacheTime time.Duration) Fs {
	c := &MonCacheOnReadFs{base: base, layer: layer, cacheTime: cacheTime}
	// TODO: Check if base fs supports some kind of notification.
	go c.monitor()
	return c
}

func (u *MonCacheOnReadFs) cacheStatus(name string) (state cacheState, fi os.FileInfo, err error) {
	var lfi, bfi os.FileInfo
	lfi, err = u.layer.Stat(name)
	if err == nil {
		if u.cacheTime == 0 {
			return cacheHit, lfi, nil
		}
		if lfi.ModTime().Add(u.cacheTime).Before(time.Now()) {
			bfi, err = u.base.Stat(name)
			if err != nil {
				return cacheLocal, lfi, nil
			}
			if bfi.ModTime().After(lfi.ModTime()) {
				return cacheStale, bfi, nil
			}
		}
		return cacheHit, lfi, nil
	}

	if err == syscall.ENOENT {
		return cacheMiss, nil, nil
	}
	var ok bool
	if err, ok = err.(*os.PathError); ok {
		if err == os.ErrNotExist {
			return cacheMiss, nil, nil
		}
	}
	return cacheMiss, nil, err
}

func (u *MonCacheOnReadFs) copyToLayer(name string) error {
	return copyToLayer(u.base, u.layer, name)
}

func (u *MonCacheOnReadFs) monitor() {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Println("[MonCacheOnReadFs] NewWatcher error:", err)
		return
	}

	go func() {
		for {
			select {
			case event := <-watcher.Events:
				if event.Op&fsnotify.Write == fsnotify.Write {
					st, fi, err := u.cacheStatus(event.Name)
					if err != nil {
						log.Println("[MonCacheOnReadFs] Cache status error:", err)
						continue
					}
					switch st {
					case cacheLocal, cacheStale, cacheHit:
						if !fi.IsDir() {
							if err := u.copyToLayer(event.Name); err != nil {
								log.Println("[MonCacheOnReadFs] Cache error:", err)
								continue
							}
						}
					}
				}
			case err := <-watcher.Errors:
				log.Println("[MonCacheOnReadFs] Watcher error:", err)
			}
		}
	}()
}

func (u *MonCacheOnReadFs) Chtimes(name string, atime, mtime time.Time) error {
	st, _, err := u.cacheStatus(name)
	if err != nil {
		return err
	}
	switch st {
	case cacheLocal:
	case cacheHit:
		err = u.base.Chtimes(name, atime, mtime)
	case cacheStale, cacheMiss:
		if err := u.copyToLayer(name); err != nil {
			return err
		}
		err = u.base.Chtimes(name, atime, mtime)
	}
	if err != nil {
		return err
	}
	return u.layer.Chtimes(name, atime, mtime)
}

func (u *MonCacheOnReadFs) Chmod(name string, mode os.FileMode) error {
	st, _, err := u.cacheStatus(name)
	if err != nil {
		return err
	}
	switch st {
	case cacheLocal:
	case cacheHit:
		err = u.base.Chmod(name, mode)
	case cacheStale, cacheMiss:
		if err := u.copyToLayer(name); err != nil {
			return err
		}
		err = u.base.Chmod(name, mode)
	}
	if err != nil {
		return err
	}
	return u.layer.Chmod(name, mode)
}

func (u *MonCacheOnReadFs) Stat(name string) (os.FileInfo, error) {
	st, fi, err := u.cacheStatus(name)
	if err != nil {
		return nil, err
	}
	switch st {
	case cacheMiss:
		return u.base.Stat(name)
	default: // cacheStale has base, cacheHit and cacheLocal the layer os.FileInfo
		return fi, nil
	}
}

func (u *MonCacheOnReadFs) Rename(oldname, newname string) error {
	st, _, err := u.cacheStatus(oldname)
	if err != nil {
		return err
	}
	switch st {
	case cacheLocal:
	case cacheHit:
		err = u.base.Rename(oldname, newname)
	case cacheStale, cacheMiss:
		if err := u.copyToLayer(oldname); err != nil {
			return err
		}
		err = u.base.Rename(oldname, newname)
	}
	if err != nil {
		return err
	}
	return u.layer.Rename(oldname, newname)
}

func (u *MonCacheOnReadFs) Remove(name string) error {
	st, _, err := u.cacheStatus(name)
	if err != nil {
		return err
	}
	switch st {
	case cacheLocal:
	case cacheHit, cacheStale, cacheMiss:
		err = u.base.Remove(name)
	}
	if err != nil {
		return err
	}
	return u.layer.Remove(name)
}

func (u *MonCacheOnReadFs) RemoveAll(name string) error {
	st, _, err := u.cacheStatus(name)
	if err != nil {
		return err
	}
	switch st {
	case cacheLocal:
	case cacheHit, cacheStale, cacheMiss:
		err = u.base.RemoveAll(name)
	}
	if err != nil {
		return err
	}
	return u.layer.RemoveAll(name)
}

func (u *MonCacheOnReadFs) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	st, _, err := u.cacheStatus(name)
	if err != nil {
		return nil, err
	}
	switch st {
	case cacheLocal, cacheHit:
	default:
		if err := u.copyToLayer(name); err != nil {
			return nil, err
		}
	}
	if flag&(os.O_WRONLY|syscall.O_RDWR|os.O_APPEND|os.O_CREATE|os.O_TRUNC) != 0 {
		bfi, err := u.base.OpenFile(name, flag, perm)
		if err != nil {
			return nil, err
		}
		lfi, err := u.layer.OpenFile(name, flag, perm)
		if err != nil {
			bfi.Close() // oops, what if O_TRUNC was set and file opening in the layer failed...?
			return nil, err
		}
		return &UnionFile{base: bfi, layer: lfi}, nil
	}
	return u.layer.OpenFile(name, flag, perm)
}

func (u *MonCacheOnReadFs) Open(name string) (File, error) {
	st, fi, err := u.cacheStatus(name)
	if err != nil {
		return nil, err
	}

	switch st {
	case cacheLocal:
		return u.layer.Open(name)

	case cacheMiss:
		bfi, err := u.base.Stat(name)
		if err != nil {
			return nil, err
		}
		if bfi.IsDir() {
			return u.base.Open(name)
		}
		if err := u.copyToLayer(name); err != nil {
			return nil, err
		}
		return u.layer.Open(name)

	case cacheStale:
		if !fi.IsDir() {
			if err := u.copyToLayer(name); err != nil {
				return nil, err
			}
			return u.layer.Open(name)
		}
	case cacheHit:
		if !fi.IsDir() {
			return u.layer.Open(name)
		}
	}
	// the dirs from cacheHit, cacheStale fall down here:
	bfile, _ := u.base.Open(name)
	lfile, err := u.layer.Open(name)
	if err != nil && bfile == nil {
		return nil, err
	}
	return &UnionFile{base: bfile, layer: lfile}, nil
}

func (u *MonCacheOnReadFs) Mkdir(name string, perm os.FileMode) error {
	err := u.base.Mkdir(name, perm)
	if err != nil {
		return err
	}
	return u.layer.MkdirAll(name, perm) // yes, MkdirAll... we cannot assume it exists in the cache
}

func (u *MonCacheOnReadFs) Name() string {
	return "CacheOnReadFs"
}

func (u *MonCacheOnReadFs) MkdirAll(name string, perm os.FileMode) error {
	err := u.base.MkdirAll(name, perm)
	if err != nil {
		return err
	}
	return u.layer.MkdirAll(name, perm)
}

func (u *MonCacheOnReadFs) Create(name string) (File, error) {
	bfh, err := u.base.Create(name)
	if err != nil {
		return nil, err
	}
	lfh, err := u.layer.Create(name)
	if err != nil {
		// oops, see comment about OS_TRUNC above, should we remove? then we have to
		// remember if the file did not exist before
		bfh.Close()
		return nil, err
	}
	return &UnionFile{base: bfh, layer: lfh}, nil
}
