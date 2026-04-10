/*
 * include/framework/structure/WriteAheadLogger.h
 * TODO: The code in this file is very poorly commented.
 */

#pragma once

#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <vector>

#include "framework/interface/Record.h"

#define WAL_PATH "wal.log"

namespace de
{
    template <RecordInterface R>
    class MutableBuffer;

    template <RecordInterface R>
    class WriteAheadLogger
    {
        enum WAL_ENTRY_TYPES
        {
            INSERT,
            DELETE,
            FLUSH_B,
            FLUSH_C
        };
        struct WAL_Entry
        {
            int lsn;
            int type;
            Wrapped<R> payload;
        };

    public:
        WriteAheadLogger() : m_wal_fd(-1), m_lsn(0)
        {
            m_wal_fd = open(WAL_PATH, O_CREAT | O_RDWR, 0644);
            if (m_wal_fd < 0)
                throw std::system_error(errno, std::generic_category(), "failed to create WAL file");
            struct stat st;
            fstat(m_wal_fd, &st);
            m_lsn = st.st_size / sizeof(WAL_Entry);
        }

        ~WriteAheadLogger()
        {
            close(m_wal_fd);
        }

        void append(Wrapped<R> &rec)
        {
            WAL_Entry entry;
            entry.lsn = m_lsn;
            entry.type = WAL_ENTRY_TYPES::INSERT;
            entry.payload = rec;

            write(entry);
        }

        void remove(Wrapped<R> &rec)
        {
            WAL_Entry entry;
            off_t offset = 0;

            while (pread(m_wal_fd, &entry, sizeof(entry), offset) == sizeof(entry))
            {
                offset += sizeof(entry);

                if (entry.payload.rec.key == rec.key && entry.payload.rec.value == rec.value)
                {
                    entry.type = WAL_ENTRY_TYPES::DELETE;
                    pwrite(m_wal_fd, &entry, sizeof(entry), offset);
                    break;
                }
            }
        }

        void flush_begin()
        {
            WAL_Entry entry;
            entry.lsn = m_lsn;
            entry.type = WAL_ENTRY_TYPES::FLUSH_B;
            entry.payload = {};

            write(entry);
        }

        void flush_complete()
        {
            WAL_Entry entry;
            entry.lsn = m_lsn;
            entry.type = WAL_ENTRY_TYPES::FLUSH_C;
            entry.payload = {};

            write(entry);

            std::vector<WAL_Entry> remaining;
            bool flush_began = false;
            off_t offset = 0;

            while (pread(m_wal_fd, &entry, sizeof(entry), offset) == sizeof(entry))
            {
                offset += sizeof(entry);
                if (entry.type == WAL_ENTRY_TYPES::FLUSH_B)
                    flush_began = true;

                if (flush_began && entry.type != WAL_ENTRY_TYPES::FLUSH_B && entry.type != WAL_ENTRY_TYPES::FLUSH_C)
                    remaining.push_back(entry);
            }

            truncate();

            for (auto &e : remaining)
                write(e);
        }

        static void build_buffer_from_log(void *buff)
        {
            MutableBuffer<R> *buffer = (MutableBuffer<R> *)buff;

            int fd = open(WAL_PATH, O_RDONLY);
            if (fd < 0)
                throw std::system_error(errno, std::generic_category(), "failed to open WAL file");

            WAL_Entry entry;
            off_t offset = 0;

            while (pread(fd, &entry, sizeof(entry), offset) == sizeof(entry))
            {
                offset += sizeof(entry);
                if (entry.type == WAL_ENTRY_TYPES::DELETE || entry.type == WAL_ENTRY_TYPES::FLUSH_B || entry.type == WAL_ENTRY_TYPES::FLUSH_C)
                    continue;

                buffer->append(entry.payload.rec, entry.payload.is_tombstone(), false);
            }

            close(fd);
        }

    private:
        void write(WAL_Entry &entry)
        {
            pwrite(m_wal_fd, &entry, sizeof(entry), m_lsn * sizeof(entry));
            fdatasync(m_wal_fd);
            m_lsn++;
        }

        void truncate()
        {
            if (ftruncate(m_wal_fd, 0) < 0)
                throw std::system_error(errno, std::generic_category(), "failed to clear WAL file");
            fsync(m_wal_fd);
            lseek(m_wal_fd, 0, SEEK_SET);
            m_lsn = 0;
        }

        int m_wal_fd;
        ssize_t m_lsn;
    };
}
