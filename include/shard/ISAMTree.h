/*
 * include/shard/ISAMTree.h
 *
 * Copyright (C) 2023-2024 Douglas B. Rumbaugh <drumbaugh@psu.edu>
 *                         Dong Xie <dongx@psu.edu>
 *
 * Distributed under the Modified BSD License.
 *
 * A shard shim around an in-memory ISAM tree.
 *
 * TODO: The code in this file is very poorly commented.
 */
#pragma once

#include <cassert>
#include <fcntl.h>
#include <queue>
#include <string>
#include <sys/stat.h>
#include <vector>

#include "framework/ShardRequirements.h"

#include "psu-ds/BloomFilter.h"
#include "util/SortedMerge.h"
#include "util/bf_config.h"
#include "uuid.h"

using psudb::BloomFilter;
using psudb::byte;
using psudb::CACHELINE_SIZE;

#define SHARD_DIR "shards/"
#define TEMP_SHARD_DIR "tmp_shards/"

namespace de
{

  template <KVPInterface R>
  class ISAMTree
  {
  private:
    typedef decltype(R::key) K;
    typedef decltype(R::value) V;
    typedef uint32_t PageNum;

    constexpr static size_t NODE_SZ = 256;
    constexpr static size_t INTERNAL_FANOUT =
        NODE_SZ / (sizeof(K) + sizeof(PageNum));

    struct ISAMTreeHeader
    {
      std::array<uint8_t, 16> id;
      PageNum root_page;
      PageNum last_data_page;
      size_t reccnt;
      size_t internal_node_cnt;
    };

    struct InternalNode
    {
      K keys[INTERNAL_FANOUT];
      PageNum child[INTERNAL_FANOUT];
    };

    struct HeapNode
    {
      Wrapped<R> record;
      size_t file_idx;

      bool operator>(const HeapNode &rhs) const
      {
        return record.rec.key > rhs.record.rec.key;
      }
    };

    static_assert(sizeof(InternalNode) == NODE_SZ, "node size does not match");

    constexpr static size_t LEAF_FANOUT = NODE_SZ / sizeof(Wrapped<R>);

  public:
    typedef R RECORD;

    ISAMTree(BufferView<R> buffer)
        : m_bf(nullptr), m_root_page(0), m_reccnt(0),
          m_tombstone_cnt(0), m_internal_node_cnt(0), m_deleted_cnt(0),
          m_alloc_size(0), m_last_data_page(0), m_isam_fd(-1)
    {
      m_id = generate_uuid();
      m_reccnt = buffer.get_record_count();
      m_tombstone_cnt = buffer.get_tombstone_count();

      initialize();

      std::vector<int> run_fds = create_sorted_runs(std::move(buffer));
      external_merge_sort(run_fds);
      cleanup_sorted_runs(run_fds);

      if (m_reccnt > 0)
      {
        build_internal_levels(1, m_last_data_page);
      }

      m_internal_node_cnt = m_root_page - m_last_data_page;

      write_header();

      std::string tmp_file = std::string(TEMP_SHARD_DIR) + get_filename();
      std::string renamed_file = std::string(SHARD_DIR) + get_filename();

      rename(tmp_file.c_str(), renamed_file.c_str());
    }

    ISAMTree(std::vector<ISAMTree *> const &shards)
        : m_bf(nullptr), m_root_page(0), m_reccnt(0),
          m_tombstone_cnt(0), m_internal_node_cnt(0), m_deleted_cnt(0),
          m_alloc_size(0), m_last_data_page(0), m_isam_fd(-1)
    {

      std::vector<int> run_fds;
      std::vector<size_t> records_per_shard;

      m_id = generate_uuid();

      for (ISAMTree *shard : shards)
      {
        m_reccnt += shard->get_record_count();
        m_tombstone_cnt += shard->get_tombstone_count();
        run_fds.push_back(shard->m_isam_fd);
        records_per_shard.push_back(shard->get_record_count());
      }

      initialize();

      external_merge_sort(run_fds, true, records_per_shard);

      if (m_reccnt > 0)
        build_internal_levels(1, m_last_data_page);

      m_internal_node_cnt = m_root_page - m_last_data_page;

      write_header();

      std::string tmp_file = std::string(TEMP_SHARD_DIR) + get_filename();
      std::string renamed_file = std::string(SHARD_DIR) + get_filename();

      rename(tmp_file.c_str(), renamed_file.c_str());
    }

    ISAMTree(std::string filename)
        : m_bf(nullptr), m_root_page(0), m_reccnt(0),
          m_tombstone_cnt(0), m_internal_node_cnt(0), m_deleted_cnt(0),
          m_alloc_size(0), m_last_data_page(0), m_isam_fd(-1)
    {
      std::string file_path = std::string(SHARD_DIR) + filename;
      int m_isam_fd = open(file_path.c_str(), O_RDONLY);
      if (m_isam_fd < 0)
        throw std::system_error(errno, std::generic_category(), "failed to open ISAM file");

      ISAMTreeHeader header;

      if (pread(m_isam_fd, &header, sizeof(ISAMTreeHeader), 0) < sizeof(ISAMTreeHeader))
        throw std::system_error(errno, std::generic_category(), "failed to read ISAM Header from ISAM file");

      m_id = uuids::uuid(header.id);
      m_root_page = header.root_page;
      m_last_data_page = header.last_data_page;
      m_reccnt = header.reccnt;
      m_internal_node_cnt = header.internal_node_cnt;
    }

    ~ISAMTree()
    {
      remove(get_filename().c_str());
      close(m_isam_fd);
      delete m_bf;
    }

    uuids::uuid get_id() const { return m_id; }

    std::string get_filename() const
    {
      return uuids::to_string(m_id) + ".dat";
    }

    Wrapped<R> *point_lookup(const R &rec, bool filter = false, std::byte *buffer = nullptr)
    {
      if (filter && !m_bf->lookup(rec))
        return nullptr;

      size_t offset = get_lower_bound(rec.key);

      if (offset >= (m_last_data_page + 1) * NODE_SZ)
        return nullptr;

      PageNum page = offset / NODE_SZ;
      psudb::sf_aligned_alloc(NODE_SZ, NODE_SZ, &buffer);

      if (pread(m_isam_fd, buffer, NODE_SZ, page * NODE_SZ) < 0)
      {
        free(buffer);
        throw std::system_error(errno, std::generic_category(), "failed to read from ISAM file in point_lookup");
      }

      size_t page_offset = (offset % NODE_SZ) / sizeof(Wrapped<R>);
      Wrapped<R> *records = reinterpret_cast<Wrapped<R> *>(buffer);

      if (records[page_offset].rec == rec)
      {
        Wrapped<R> *result = new Wrapped<R>(records[page_offset]);
        return result;
      }

      free(buffer);
      return nullptr;
    }

    size_t get_record_count() const { return m_reccnt; }

    size_t get_tombstone_count() const { return m_tombstone_cnt; }

    size_t get_memory_usage() const { return m_internal_node_cnt * NODE_SZ; }

    size_t get_aux_memory_usage() const { return (m_bf) ? m_bf->memory_usage() : 0; }

    /* SortedShardInterface methods */
    ssize_t get_lower_bound(const K &key) const
    {
      PageNum now = m_root_page;
      std::byte *buffer = nullptr;
      psudb::sf_aligned_alloc(NODE_SZ, NODE_SZ, &buffer);

      while (!is_leaf(now))
      {
        if (pread(m_isam_fd, buffer, NODE_SZ, now * NODE_SZ) < 0)
        {
          free(buffer);
          throw std::system_error(errno, std::generic_category(), "failed to read from ISAM file in get_lower_bound");
        }

        InternalNode *node = (InternalNode *)buffer;
        PageNum next = node->child[0];

        for (size_t i = 0; i < INTERNAL_FANOUT - 1; i++)
        {
          if (node->child[i] == 0)
            break;

          next = node->child[i];

          if (key < node->keys[i])
            break;
        }

        now = next;
      }

      if (pread(m_isam_fd, buffer, NODE_SZ, now * NODE_SZ) < 0)
      {
        free(buffer);
        throw std::system_error(errno, std::generic_category(), "failed to read from ISAM file in get_lower_bound");
      }

      const Wrapped<R> *pos = reinterpret_cast<const Wrapped<R> *>(buffer);
      size_t offset = LEAF_FANOUT;

      for (size_t i = 0; i < LEAF_FANOUT; i++)
      {
        if (pos->rec.key >= key)
        {
          offset = i;
          break;
        }
        pos++;
      }

      free(buffer);
      return now * NODE_SZ + offset * sizeof(Wrapped<R>);
    }

    ssize_t get_upper_bound(const K &key) const
    {
      PageNum now = m_root_page;
      std::byte *buffer = nullptr;
      psudb::sf_aligned_alloc(NODE_SZ, NODE_SZ, &buffer);

      while (!is_leaf(now))
      {
        if (pread(m_isam_fd, buffer, NODE_SZ, now * NODE_SZ) < 0)
        {
          free(buffer);
          throw std::system_error(errno, std::generic_category(), "failed to read from ISAM file in get_upper_bound");
        }

        InternalNode *node = (InternalNode *)buffer;
        PageNum next = node->child[0];

        for (size_t i = 0; i < INTERNAL_FANOUT - 1; i++)
        {
          if (node->child[i] == 0)
            break;

          next = node->child[i];

          if (key < node->keys[i])
            break;
        }

        now = next;
      }

      if (pread(m_isam_fd, buffer, NODE_SZ, now * NODE_SZ) < 0)
      {
        free(buffer);
        throw std::system_error(errno, std::generic_category(), "failed to read from ISAM file in get_upper_bound");
      }

      const Wrapped<R> *pos = reinterpret_cast<const Wrapped<R> *>(buffer);
      size_t offset = LEAF_FANOUT;

      for (size_t i = 0; i < LEAF_FANOUT; i++)
      {
        if (pos->rec.key > key)
        {
          offset = i;
          break;
        }
        pos++;
      }

      free(buffer);
      return now * NODE_SZ + offset * sizeof(Wrapped<R>);
    }

    const Wrapped<R> *get_record_at(size_t idx, std::byte *buffer) const
    {
      PageNum page_num = idx / LEAF_FANOUT + 1;

      if (page_num > m_root_page)
        return nullptr;

      psudb::sf_aligned_alloc(NODE_SZ, NODE_SZ, &buffer);

      size_t bytes = pread(m_isam_fd, buffer, NODE_SZ, page_num * NODE_SZ);

      if (bytes < 0)
        throw std::system_error(errno, std::generic_category(), "failed to read from ISAM file in get_record_at");

      auto records = reinterpret_cast<Wrapped<R> *>(buffer);
      size_t idx_in_page = idx % LEAF_FANOUT;

      if (idx_in_page >= LEAF_FANOUT)
        return nullptr;

      return &records[idx_in_page];
    }

  private:
    /**
     * Move this to a more appropriate place later.
     */
    uuids::uuid generate_uuid()
    {
      std::random_device rd;
      auto seed_data = std::array<int, std::mt19937::state_size>{};
      std::generate(std::begin(seed_data), std::end(seed_data), std::ref(rd));
      std::seed_seq seq(std::begin(seed_data), std::end(seed_data));
      std::mt19937 generator(seq);
      uuids::uuid_random_generator gen{generator};

      uuids::uuid const id = gen();
      assert(!id.is_nil());
      assert(id.as_bytes().size() == 16);
      assert(id.version() == uuids::uuid_version::random_number_based);
      assert(id.variant() == uuids::uuid_variant::rfc);

      return id;
    }

    void initialize()
    {
      std::string shard_filename = get_filename();

      if (mkdir(SHARD_DIR, 0755) < 0 && errno != EEXIST)
      {
        throw std::system_error(errno, std::generic_category(), "failed to create shard directory");
      }

      if (mkdir(TEMP_SHARD_DIR, 0755) < 0 && errno != EEXIST)
      {
        throw std::system_error(errno, std::generic_category(), "failed to create shard directory");
      }

      std::string tmp_file_path = std::string(TEMP_SHARD_DIR) + shard_filename;

      m_isam_fd = open(tmp_file_path.c_str(), O_CREAT | O_RDWR, 0644);
      if (m_isam_fd < 0)
      {
        throw std::system_error(errno, std::generic_category(), "failed to create ISAM file");
      }
    }

    std::vector<int> create_sorted_runs(BufferView<R> bv, size_t size_of_chunk = LEAF_FANOUT)
    {
      std::vector<int> run_fds;

      std::string tmp_dir = "isam_tmp_" + uuids::to_string(m_id);

      if (mkdir(tmp_dir.c_str(), 0755) < 0 && errno != EEXIST)
      {
        throw std::system_error(errno, std::generic_category(), "failed to create temporary directory for sorting");
      }

      for (size_t start = 0; start < m_reccnt; start += LEAF_FANOUT)
      {
        size_t chunk_size = std::min(size_of_chunk, m_reccnt - start);

        Wrapped<R> *chunk = nullptr;
        psudb::sf_aligned_alloc(NODE_SZ, NODE_SZ, (byte **)&chunk);

        for (size_t i = 0; i < chunk_size; i++)
        {
          chunk[i] = *bv.get(start + i);
        }

        std::sort(chunk, chunk + chunk_size,
                  [](const Wrapped<R> &a, const Wrapped<R> &b)
                  {
                    return a.rec < b.rec;
                  });

        std::string tmp_file = tmp_dir + std::to_string(run_fds.size());

        int tmp_fd = open(tmp_file.c_str(), O_CREAT | O_RDWR, 0644);
        if (tmp_fd < 0)
        {
          free(chunk);
          throw std::system_error(errno, std::generic_category(), "failed to create temporary file for sorted runs");
        }

        if (pwrite(tmp_fd, chunk, NODE_SZ, 0) < 0)
        {
          close(tmp_fd);
          free(chunk);
          throw std::system_error(errno, std::generic_category(), "failed to write to temporary files for sorted runs");
        }

        remove(tmp_file.c_str());

        run_fds.push_back(tmp_fd);
        free(chunk);
      }
      rmdir(tmp_dir.c_str());

      return run_fds;
    }

    void cleanup_sorted_runs(const std::vector<int> &run_fds)
    {
      for (int fd : run_fds)
        close(fd);
    }

    off_t advance_offset(off_t curr, bool shards = false)
    {
      curr += sizeof(Wrapped<R>);

      if (!shards)
        return curr;

      if (NODE_SZ - (curr % NODE_SZ) < sizeof(Wrapped<R>))
        curr += NODE_SZ - (curr % NODE_SZ);

      return curr;
    }

    void external_merge_sort(const std::vector<int> &run_fds, bool shards = false, const std::vector<size_t> &records_per_shard = {})
    {
      std::byte *buffer = nullptr;
      std::priority_queue<HeapNode, std::vector<HeapNode>, std::greater<HeapNode>> heap;
      std::vector<off_t> file_offsets(run_fds.size(), shards ? NODE_SZ : 0);
      std::vector<size_t> remaining(records_per_shard);

      for (size_t i = 0; i < run_fds.size(); i++)
      {
        Wrapped<R> rec;
        if (pread(run_fds[i], &rec, sizeof(Wrapped<R>), file_offsets[i]) < 0)
        {
          throw std::system_error(errno, std::generic_category(), "failed to read from sorted run file");
        }

        heap.push({rec, i});
        if (!remaining.empty())
          remaining[i]--;

        file_offsets[i] = advance_offset(file_offsets[i], shards);
      }

      psudb::sf_aligned_alloc(NODE_SZ, NODE_SZ, &buffer);
      auto page = reinterpret_cast<Wrapped<R> *>(buffer);

      size_t count = 0;
      PageNum curr_page = 1;

      while (!heap.empty())
      {
        HeapNode min = heap.top();
        heap.pop();

        if (count == LEAF_FANOUT)
        {
          if (pwrite(m_isam_fd, page, NODE_SZ, curr_page * NODE_SZ) < 0)
          {
            throw std::system_error(errno, std::generic_category(), "failed to write to ISAM file during external merge sort");
          }

          memset(page, 0, NODE_SZ);
          count = 0;
          curr_page++;
        }

        page[count++] = min.record;

        Wrapped<R> next;
        if (remaining.empty() || remaining[min.file_idx] > 0)
        {
          if (pread(run_fds[min.file_idx], &next, sizeof(Wrapped<R>), file_offsets[min.file_idx]) == sizeof(Wrapped<R>))
          {
            heap.push({next, min.file_idx});
            if (!remaining.empty())
              remaining[min.file_idx]--;
            file_offsets[min.file_idx] = advance_offset(file_offsets[min.file_idx], shards);
          }
        }
      }

      if (count > 0)
      {
        if (pwrite(m_isam_fd, page, NODE_SZ, curr_page * NODE_SZ) < 0)
        {
          throw std::system_error(errno, std::generic_category(), "failed to write last page to ISAM file during external merge sort");
        }
      }

      m_last_data_page = curr_page;

      free(buffer);
    }

    void build_internal_levels(PageNum first_page, PageNum last_page)
    {
      if (first_page == last_page)
      {
        m_root_page = first_page;
        return;
      }

      PageNum write_page = last_page + 1;
      PageNum child_page = first_page;

      while (child_page <= last_page)
      {
        InternalNode *node = nullptr;
        psudb::sf_aligned_alloc(NODE_SZ, NODE_SZ, (byte **)&node);

        for (size_t i = 0; i < INTERNAL_FANOUT; i++)
        {
          if (child_page > last_page)
            break;

          std::byte *buffer = nullptr;
          psudb::sf_aligned_alloc(NODE_SZ, NODE_SZ, &buffer);
          if (pread(m_isam_fd, buffer, NODE_SZ, child_page * NODE_SZ) < 0)
          {
            throw std::system_error(errno, std::generic_category(), "failed to read from ISAM file while building internal levels");
          }

          size_t records_on_page = LEAF_FANOUT;

          if (child_page <= m_last_data_page)
          {
            records_on_page = std::min(LEAF_FANOUT, m_reccnt - (child_page - 1) * LEAF_FANOUT);
          }

          Wrapped<R> *records = (Wrapped<R> *)(buffer);
          node->keys[i] = records[records_on_page - 1].rec.key;
          node->child[i] = child_page;
          child_page++;
          free(buffer);
        }

        if (pwrite(m_isam_fd, node, NODE_SZ, write_page * NODE_SZ) < 0)
        {
          throw std::system_error(errno, std::generic_category(), "failed to write internal node to ISAM file");
        }
        free(node);
        write_page++;
      }
      build_internal_levels(last_page + 1, write_page - 1);
    }

    void write_header()
    {
      ISAMTreeHeader header;
      std::memcpy(header.id.data(), m_id.as_bytes().data(), 16);
      header.root_page = m_root_page;
      header.last_data_page = m_last_data_page;
      header.reccnt = m_reccnt;
      header.internal_node_cnt = m_internal_node_cnt;

      if (pwrite(m_isam_fd, &header, sizeof(header), 0) < 0)
      {
        throw std::runtime_error("failed to write ISAM tree header");
      }
    }

    bool is_leaf(PageNum page_num) const
    {
      return page_num >= 1 && page_num <= m_last_data_page;
    }

    psudb::BloomFilter<R> *m_bf;
    PageNum m_root_page;
    PageNum m_last_data_page;
    size_t m_reccnt;
    size_t m_tombstone_cnt;
    size_t m_internal_node_cnt;
    size_t m_deleted_cnt;
    size_t m_alloc_size;
    uuids::uuid m_id;
    int m_isam_fd;
  };
} // namespace de
