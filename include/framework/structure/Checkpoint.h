#pragma once

#include <dirent.h>
#include <fcntl.h>
#include <nlohmann/json.hpp>
#include <string>
#include <sys/stat.h>
#include <unistd.h>

#include "framework/util/Configuration.h"
#include "framework/structure/InternalLevel.h"

#define CHECKPOINT_PATH "checkpoint.json"
#define CHECKPOINT_TMP_PATH "checkpoint_tmp.json"

#define TEMP_SHARD_DIR "tmp_shards/"

namespace de
{
    template <ShardInterface ShardType, QueryInterface<ShardType> QueryType,
              LayoutPolicy L = LayoutPolicy::TEIRING>
    void save_checkpoint(std::vector<std::shared_ptr<InternalLevel<ShardType, QueryType>>> levels)
    {
        nlohmann::json j;
        for (size_t i = 0; i < levels.size(); i++)
        {
            nlohmann::json shards = nlohmann::json::array();
            for (size_t k = 0; k < levels[i]->get_shard_count(); k++)
            {
                auto shard = levels[i]->get_shard(k);
                shards.push_back(shard->get_filename());
            }
            j[std::to_string(i)] = shards;
        }

        std::string ckpnt = j.dump(4);
        int fd = open(CHECKPOINT_TMP_PATH, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd < 0)
            throw std::system_error(errno, std::generic_category(), "failed to create/open checkpoint file");

        if (pwrite(fd, ckpnt.c_str(), ckpnt.size(), 0) < 0)
        {
            close(fd);
            throw std::system_error(errno, std::generic_category(), "failed to write to checkpoint file");
        }

        fdatasync(fd);
        close(fd);

        if (rename(CHECKPOINT_TMP_PATH, CHECKPOINT_PATH) < 0)
            throw std::system_error(errno, std::generic_category(), "failed to create/open checkpoint file");
    }

    template <ShardInterface ShardType, QueryInterface<ShardType> QueryType,
              LayoutPolicy L = LayoutPolicy::TEIRING>
    void load_checkpoint(std::vector<std::shared_ptr<InternalLevel<ShardType, QueryType>>> &levels)
    {

        if (access(CHECKPOINT_PATH, F_OK) < 0)
            return;

        int fd = open(CHECKPOINT_PATH, O_RDONLY);
        if (fd < 0)
            throw std::system_error(errno, std::generic_category(), "failed to open checkpoint file");

        std::string content;
        struct stat st;
        fstat(fd, &st);
        content.resize(st.st_size);
        if (pread(fd, content.data(), st.st_size, 0) < st.st_size)
        {
            close(fd);
            throw std::system_error(errno, std::generic_category(), "failed to read from checkpoint file");
        }
        nlohmann::json j = nlohmann::json::parse(content);
        close(fd);

        for (auto &[idx, shards] : j.items())
        {
            int level = std::stoi(idx);

            while (levels.size() <= level)
                levels.emplace_back(nullptr);

            levels[level] = std::make_shared<InternalLevel<ShardType, QueryType>>(level, shards.size());

            for (auto &shard : shards)
                levels[level]->append_shard(shard.get<std::string>());
        }

        DIR *dir = opendir(TEMP_SHARD_DIR);
        if (dir == NULL)
            return;

        struct dirent *entry;
        while ((entry = readdir(dir)) != NULL)
        {
            std::string tmp_file = std::string(TEMP_SHARD_DIR) + entry->d_name;
            remove(tmp_file.c_str());
        }
    }
}