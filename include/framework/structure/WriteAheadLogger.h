/*
 * include/framework/structure/WriteAheadLogger.h
 * TODO: The code in this file is very poorly commented.
 */

#pragma once

#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include "framework/interface/Record.h"

#define WAL_PATH "wal.log"

namespace de
{
    template <RecordInterface R>
    class MutableBuffer;

    template <RecordInterface R>
    class WriteAheadLogger
    {
    public:
        WriteAheadLogger() : m_wal_fd(-1), m_lsn(0)
        {

            m_wal_fd = open(WAL_PATH, O_CREAT | O_RDWR, 0644);
            if (m_wal_fd < 0)
                throw std::system_error(errno, std::generic_category(), "failed to create WAL file");
            struct stat st;
            fstat(m_wal_fd, &st);
            m_lsn = st.st_size / sizeof(Wrapped<R>);
        }

        ~WriteAheadLogger()
        {
            close(m_wal_fd);
        }

        int add(Wrapped<R> &rec)
        {
            if (pwrite(m_wal_fd, &rec, sizeof(rec), m_lsn * sizeof(rec)) < 0)
                throw std::system_error(errno, std::generic_category(), "failed to write to WAL file");

            fdatasync(m_wal_fd);
            m_lsn++;

            return 0;
        }

        int truncate()
        {
            if (ftruncate(m_wal_fd, 0) < 0)
                throw std::system_error(errno, std::generic_category(), "failed to clear WAL file");

            fsync(m_wal_fd);
            lseek(m_wal_fd, 0, SEEK_SET);

            m_lsn = 0;

            return 0;
        }

        static void build_buffer_from_log(void *buff)
        {
            MutableBuffer<R> *buffer = (MutableBuffer<R> *)buff;

            int fd = open(WAL_PATH, O_RDONLY);
            if (fd < 0)
                throw std::system_error(errno, std::generic_category(), "failed to open WAL file");

            Wrapped<R> wrec;
            off_t offset = 0;

            while (pread(fd, &wrec, sizeof(wrec), offset) == sizeof(wrec))
            {
                offset += sizeof(wrec);
                buffer->append(wrec.rec, wrec.is_tombstone(), false);
            }

            close(fd);
        }

    private:
        int m_wal_fd;
        ssize_t m_lsn;
    };
}
