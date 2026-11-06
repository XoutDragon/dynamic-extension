/*
 * include/framework/structure/WriteAheadLogger.h
 * TODO: The code in this file is very poorly commented.
 */

#pragma once

#include <cstdio>
#include <fcntl.h>
#include <sys/stat.h>
#include <system_error>
#include <threads.h>
#include <unistd.h>

#include <algorithm>
#include <vector>

#include "framework/interface/Record.h"

#define WAL_PATH "wal.log"

namespace de {
template <RecordInterface R> class MutableBuffer;

template <RecordInterface R> class WriteAheadLogger {
  enum WAL_ENTRY_TYPES { INSERT, DELETE, FLUSH_B, FLUSH_C };
  struct WAL_Entry {
    int type;
    Wrapped<R> payload;
  };

public:
  WriteAheadLogger() : m_wal_fd(-1), m_lsn(0) {
    m_wal_fd = open(WAL_PATH, O_CREAT | O_RDWR, 0644);
    if (m_wal_fd < 0)
      perror("Creation of WAL failed");

    init_mtx();

    struct stat st;
    fstat(m_wal_fd, &st);
    m_lsn = st.st_size / sizeof(WAL_Entry);
  }

  ~WriteAheadLogger() {
    close(m_wal_fd);
    mtx_destroy(&m_mtx_lk);
  }

  void append(Wrapped<R> &rec) {
    mtx_lock(&m_mtx_lk);
    WAL_Entry entry;
    entry.type = WAL_ENTRY_TYPES::INSERT;
    entry.payload = rec;

    write(entry);
    mtx_unlock(&m_mtx_lk);
  }

  void remove(Wrapped<R> &rec) {
    mtx_lock(&m_mtx_lk);
    WAL_Entry entry;
    entry.type = WAL_ENTRY_TYPES::DELETE;
    entry.payload = rec;

    write(entry);
    mtx_unlock(&m_mtx_lk);
  }

  void flush_begin() {
    mtx_lock(&m_mtx_lk);
    WAL_Entry entry;
    entry.type = WAL_ENTRY_TYPES::FLUSH_B;
    entry.payload = {};

    write(entry);
    mtx_unlock(&m_mtx_lk);
  }

  void flush_complete() {
    mtx_lock(&m_mtx_lk);
    WAL_Entry entry;
    entry.type = WAL_ENTRY_TYPES::FLUSH_C;
    entry.payload = {};

    write(entry);

    std::vector<WAL_Entry> remaining;
    bool flush_began = false;
    off_t offset = 0;

    while (pread(m_wal_fd, &entry, sizeof(entry), offset) == sizeof(entry)) {
      offset += sizeof(entry);
      if (entry.type == WAL_ENTRY_TYPES::FLUSH_B)
        flush_began = true;

      if (flush_began && entry.type != WAL_ENTRY_TYPES::FLUSH_B &&
          entry.type != WAL_ENTRY_TYPES::FLUSH_C)
        remaining.push_back(entry);
    }

    truncate();

    for (auto &e : remaining)
      write(e);
    mtx_unlock(&m_mtx_lk);
  }

  static void build_buffer_from_log(void *buff) {
    MutableBuffer<R> *buffer = (MutableBuffer<R> *)buff;

    int fd = open(WAL_PATH, O_RDONLY);
    if (fd < 0)
      throw std::system_error(errno, std::generic_category(),
                              "failed to open WAL file");

    WAL_Entry entry;
    off_t offset = 0;

    std::vector<WAL_Entry> entries;
    std::vector<R> deleted;

    while (pread(fd, &entry, sizeof(entry), offset) == sizeof(entry)) {
      offset += sizeof(entry);
      if (entry.type == WAL_ENTRY_TYPES::FLUSH_B ||
          entry.type == WAL_ENTRY_TYPES::FLUSH_C)
        continue;

      if (entry.type == WAL_ENTRY_TYPES::DELETE)
        deleted.push_back(entry.payload.rec);
      else
        entries.push_back(entry);
    }

    close(fd);
    for (auto &e : entries) {
      if (std::find_if(deleted.begin(), deleted.end(), [e](const R &r) {
            return e.payload.rec.key == r.key && e.payload.rec.value == r.value;
          }) == deleted.end()) {
        buffer->append(e.payload.rec, e.payload.is_tombstone(), false);
      }
    }
  }

private:
  void init_mtx() { mtx_init(&m_mtx_lk, mtx_plain); }

  void write(WAL_Entry &entry) {
    pwrite(m_wal_fd, &entry, sizeof(entry), m_lsn * sizeof(entry));
    fdatasync(m_wal_fd);
    m_lsn++;
  }

  void truncate() {
    if (ftruncate(m_wal_fd, 0) < 0)
      throw std::system_error(errno, std::generic_category(),
                              "failed to clear WAL file");
    fsync(m_wal_fd);
    lseek(m_wal_fd, 0, SEEK_SET);
    m_lsn = 0;
  }
  mtx_t m_mtx_lk;
  int m_wal_fd;
  ssize_t m_lsn;
};
} // namespace de
