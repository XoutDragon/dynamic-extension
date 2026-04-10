// clang++ -std=c++20 -Iinclude -Itests/include -Iexternal/psudb-common/cpp/include tests2/simulation.cpp -Iexternal/stduuid/include -Iexternal/json/include -o bin/simulation -lgsl -lgslcblas -latomic -ggdb

#include <fcntl.h>
#include <unistd.h>
#include <filesystem>
#include <iostream>
#include <fstream>

#include "testing.h"
#include "shard/ISAMTree.h"
#include "framework/DynamicExtension.h"
#include "query/rangequery.h"

using json = nlohmann::json;
using namespace de;

typedef Rec R;
typedef ISAMTree<R> S;
typedef rq::Query<S> Q;

#define WAL_FILE "wal.log"
#define CHECKPOINT_FILE "checkpoint.json"

enum string_code
{
    HELP,
    CHECK_WAL,
    CHECK_CK,
    GET_RECCNT,
    FILL_BUFFER,
    SIMULATE_CRASH,
    INSERT,
    UNKNOWN
};

string_code get_command_code(const std::string &cmd)
{
    if (cmd == "check_wal")
        return CHECK_WAL;
    else if (cmd == "help")
        return HELP;
    else if (cmd == "check_ck")
        return CHECK_CK;
    else if (cmd == "get_reccnt")
        return GET_RECCNT;
    else if (cmd == "simulate_crash")
        return SIMULATE_CRASH;
    else if (cmd == "fill_buffer")
        return FILL_BUFFER;
    else if (cmd == "insert")
        return INSERT;
    else
        return UNKNOWN;
}

void simulate_crash()
{
    for (long long int i = 0; ++i; (&i)[i] = i)
        ;
}

void get_reccnt(DynamicExtension<S, Q> *de)
{
    std::cout << "Record count: " << de->get_record_count() << '\n';
}

void check_ck()
{
    if (!std::filesystem::exists(CHECKPOINT_FILE))
    {
        std::cout << "Checkpoint file does not exist.\n";
        return;
    }

    std::ifstream ck_file(CHECKPOINT_FILE);
    json ck_json;
    ck_file >> ck_json;

    std::cout << "================= Checkpoint  =================\n";
    std::cout << ck_json.dump(4) << '\n';
}

void check_wal()
{
    if (access(WAL_FILE, F_OK) < 0)
    {
        std::cout << "WAL file does not exist.\n";
        return;
    }

    int fd = open(WAL_FILE, O_RDONLY);
    struct WAL_Entry
    {
        int lsn;
        int type;
        Wrapped<R> payload;
    };

    WAL_Entry entry;

    std::cout << "================= Write Ahead Log  =================\n";

    while (read(fd, &entry, sizeof(entry)) == sizeof(entry))
    {
        std::cout << "Key: " << entry.payload.rec.key << ", Value: " << entry.payload.rec.value << '\n'
                  << ", Type: " << entry.type;
    }

    close(fd);
}

void fill_buffer(DynamicExtension<S, Q> *de, size_t buffer_low_watermark)
{
    int buffer_record_cnt = 0;

    if (access(WAL_FILE, F_OK) < 0)
    {
        std::cout << "WAL file does not exist.\n";
        return;
    }

    int fd = open(WAL_FILE, O_RDONLY);

    Wrapped<R> wrec;

    while (read(fd, &wrec, sizeof(wrec)) == sizeof(wrec))
    {
        buffer_record_cnt++;
    }

    close(fd);

    for (size_t i = 0; i <= buffer_low_watermark - buffer_record_cnt; i++)
    {
        R rec;
        rec.key = i * 100;
        rec.value = i * 100;
        de->insert(rec);
    }
}

void insert_record(DynamicExtension<S, Q> *de, uint64_t key, uint32_t value)
{
    R rec;
    rec.key = key;
    rec.value = value;

    de->insert(rec);
}

int main()
{
    size_t buffer_low_watermark = 1000;
    size_t buffer_high_watermark = 1000;
    size_t scale_factor = 3;

    DynamicExtension<S, Q> *de = new DynamicExtension<S, Q>(buffer_low_watermark, buffer_high_watermark, scale_factor);

    while (true)
    {
        std::cout << ">>> ";
        std::string line;
        std::getline(std::cin, line);

        switch (get_command_code(line))
        {
        case CHECK_WAL:
            check_wal();
            break;
        case FILL_BUFFER:
            fill_buffer(de, buffer_low_watermark);
            break;
        case INSERT:
        {
            uint64_t key;
            uint32_t value;
            std::cout << "Enter key: ";
            std::cin >> key;
            std::cout << "Enter value: ";
            std::cin >> value;
            insert_record(de, key, value);
            std::cin.ignore();
            break;
        }
        case CHECK_CK:
            check_ck();
            break;
        case GET_RECCNT:
            get_reccnt(de);
            break;
        case SIMULATE_CRASH:
        {
            std::cout << "I crash now. " << std::endl;
            simulate_crash();
            break;
        }
        case HELP:
            std::cout << "Available commands:\n";
            std::cout << "check_wal, check_ck, get_reccnt, fill_buffer, insert, simulate_crash, help\n";
            break;
        default:
            std::cout << "Unknown command.\n";
        }
        std::cout << '\n';
    }
}
