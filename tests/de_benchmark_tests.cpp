#define ENABLE_TIMER

#include <cassert>
#include <dirent.h>
#include <fcntl.h>
#include <iostream>
#include <numeric>
#include <vector>

#include "framework/DynamicExtension.h"
#include "framework/scheduling/SerialScheduler.h"
#include "include/testing.h"
#include "psu-util/timer.h"
#include "query/pointlookup.h"
#include "shard/ExternalISAMTree.h"

#include <check.h>

using namespace de;
typedef Rec R;
typedef ISAMTree<R> S;
typedef pl::Query<S> Q;
typedef DynamicExtension<S, Q, LayoutPolicy::TEIRING, DeletePolicy::TAGGING, SerialScheduler> DE;

static const size_t BUFFER_LOW_WATERMARK = 1000;
static const size_t BUFFER_HIGH_WATERMARK = 1100;
static const size_t SCALE_FACTOR = 5;
static const size_t N = 10000;
static const size_t ITERATIONS = 10;
static const size_t MEMORY_BUDGET = 0;
static const size_t THREAD_CNT = 16;

void cleanup()
{
    DIR *dir = opendir(SHARD_DIR);

    if (dir != NULL)
    {
        struct dirent *entry;
        while ((entry = readdir(dir)) != NULL)
        {
            if (dir == NULL)
                return;
            std::string filename = std::string(SHARD_DIR) + entry->d_name;
            remove(filename.c_str());
        }
    }

    rmdir(TEMP_SHARD_DIR);
    rmdir(SHARD_DIR);
    remove(CHECKPOINT_PATH);
    remove(WAL_PATH);
}

START_TEST(t_insert_benchmark)
{
    std::vector<long> times;
    for (int i = 0; i < ITERATIONS; i++)
    {
        auto de = new DE(BUFFER_LOW_WATERMARK, BUFFER_HIGH_WATERMARK, SCALE_FACTOR, MEMORY_BUDGET, THREAD_CNT, false);

        TIMER_INIT();
        TIMER_START();

        for (size_t j = 0; j < N; j++)
        {
            R rec;
            rec.key = j;
            rec.value = j;
            de->insert(rec);
        }

        TIMER_STOP();

        times.push_back(TIMER_RESULT());
        cleanup();
    }

    double average = std::reduce(times.begin(), times.end()) / ITERATIONS;

    printf("Insertions: %ld\nWAL: True\nCheckpointing: True\nAverage Time(Nanoseconds) %.0f\nAverage Time(Seconds) %f\n",
           N, average, average / 1e9);
}
END_TEST

START_TEST(t_insert_benchmark_no_fault_tolerance)
{
    std::vector<long> times;
    for (int i = 0; i < ITERATIONS; i++)
    {
        auto de = new DE(BUFFER_LOW_WATERMARK, BUFFER_HIGH_WATERMARK, SCALE_FACTOR, MEMORY_BUDGET, THREAD_CNT, true);

        TIMER_INIT();
        TIMER_START();

        for (size_t j = 0; j < N; j++)
        {
            R rec;
            rec.key = j;
            rec.value = j;
            de->insert(rec, false);
        }

        TIMER_STOP();

        times.push_back(TIMER_RESULT());
        cleanup();
    }

    double average = std::reduce(times.begin(), times.end()) / ITERATIONS;

    printf("Insertions: %ld\nWAL: False\nCheckpointing: False\nAverage Time(Nanoseconds) %.0f\nAverage Time(Seconds) %f\n",
           N, average, average / 1e9);
}
END_TEST

START_TEST(t_pointlookup_benchmark)
{
    std::vector<long> times;
    auto de = new DE(BUFFER_LOW_WATERMARK, BUFFER_HIGH_WATERMARK, SCALE_FACTOR, MEMORY_BUDGET, THREAD_CNT, true);
    for (size_t i = 0; i < BUFFER_LOW_WATERMARK + 10; i++)
    {
        R rec;
        rec.key = i;
        rec.value = i;
        de->insert(rec, false);
    }
    std::byte *buff = nullptr;
    psudb::sf_aligned_alloc(4096, 4096, &buff);

    for (int i = 0; i < ITERATIONS; i++)
    {
        long timer_count = 0;
        TIMER_INIT();
        for (size_t j = 0; j < de->get_record_count(); j++)
        {
            Q::Parameters p;
            p.search_key = (uint64_t)j;
            TIMER_START();
            auto res = de->query(std::move(p), buff).get();
            TIMER_STOP();
            timer_count += TIMER_RESULT();
        }
        times.push_back(timer_count / de->get_record_count());
    }

    double average = std::reduce(times.begin(), times.end()) / ITERATIONS;
    printf("Record count: %d\nAverage point-lookup time (Nanoseconds): %.0f\nAverage point-lookup time (Seconds): %f\n", de->get_record_count(), average, average / 1e9);

    cleanup();
    free(buff);
    delete de;
}
END_TEST

Suite *unit_testing()
{
    Suite *unit = suite_create("Dynamic Extension Benchmarking");
    TCase *test = tcase_create("de::Dynamic Extension Benchmarking");
    tcase_add_test(test, t_insert_benchmark);
    tcase_add_test(test, t_insert_benchmark_no_fault_tolerance);
    tcase_add_test(test, t_pointlookup_benchmark);
    tcase_set_timeout(test, 0);
    suite_add_tcase(unit, test);
    return unit;
}

int shard_unit_tests()
{
    int failed = 0;
    Suite *unit = unit_testing();
    SRunner *unit_shardner = srunner_create(unit);

    srunner_run_all(unit_shardner, CK_NORMAL);
    failed = srunner_ntests_failed(unit_shardner);
    srunner_free(unit_shardner);

    return failed;
}

int main()
{
    int unit_failed = shard_unit_tests();

    return (unit_failed == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
}
