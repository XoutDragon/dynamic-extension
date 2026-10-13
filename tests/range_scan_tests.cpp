/*
 * tests/range_scan_tests.cpp
 *
 * Unit tests for DynamicExtension::range_scan()
 */
#include <dirent.h>
#include <vector>

#include "framework/DynamicExtension.h"
#include "framework/scheduling/SerialScheduler.h"
#include "include/testing.h"
#include "query/pointlookup.h"
#include "shard/ExternalISAMTree.h"

#include <check.h>

using namespace de;
typedef Rec R;
typedef ISAMTree<R> S;
typedef pl::Query<S> Q;
typedef DynamicExtension<S, Q, LayoutPolicy::TEIRING, DeletePolicy::TOMBSTONE, SerialScheduler> DE;

void cleanup()
{
    DIR *dir = opendir(SHARD_DIR);

    if (dir != NULL)
    {
        struct dirent *entry;
        while ((entry = readdir(dir)) != NULL)
        {
            std::string filename = std::string(SHARD_DIR) + entry->d_name;
            remove(filename.c_str());
        }
        closedir(dir);
    }

    rmdir(TEMP_SHARD_DIR);
    rmdir(SHARD_DIR);
    remove(CHECKPOINT_PATH);
    remove(WAL_PATH);
}

/* drain an iterator into a vector */
static std::vector<R> scan(DE *de, uint64_t lo, uint64_t hi)
{
    std::vector<R> out;
    auto it = de->range_scan({lo, 0}, {hi, 0});
    while (it.has_next())
        out.push_back(it.next());
    return out;
}

/* assert that a scan returned exactly the expected records, in order */
static void check_eq(const std::vector<R> &got, const std::vector<R> &expected)
{
    ck_assert_int_eq(got.size(), expected.size());
    for (size_t i = 0; i < expected.size(); i++)
    {
        ck_assert_int_eq(got[i].key, expected[i].key);
        ck_assert_int_eq(got[i].value, expected[i].value);
    }
}

START_TEST(t_empty)
{
    cleanup();
    auto de = new DE(100, 200, 2, 0, 1, true);

    ck_assert_int_eq(scan(de, 0, 1000).size(), 0);

    delete de;
    cleanup();
}
END_TEST

START_TEST(t_buffer_only)
{
    cleanup();
    auto de = new DE(100, 200, 2, 0, 1, true);

    /* insert out of order; well below the flush threshold */
    for (uint64_t k : {5, 1, 9, 3, 7})
        de->insert({k, (uint32_t)k}, false);

    check_eq(scan(de, 3, 7), {{3, 3}, {5, 5}, {7, 7}});

    delete de;
    cleanup();
}
END_TEST

START_TEST(t_shards_and_buffer)
{
    cleanup();
    auto de = new DE(100, 200, 2, 0, 1, true);

    /* enough records to flush into shards, plus some left in the buffer */
    size_t n = 1000;
    std::vector<R> expected(n);
    for (uint64_t i = 0; i < n; i++)
    {
        uint64_t k = (i * 7) % n;
        de->insert({k, (uint32_t)i}, false);
        expected[k] = {k, (uint32_t)i};
    }

    /* make sure records were actually flushed out of the buffer */
    ck_assert_int_gt(de->get_height(), 0);

    check_eq(scan(de, 0, n), expected);
    check_eq(scan(de, 100, 199),
             std::vector<R>(expected.begin() + 100, expected.begin() + 200));

    delete de;
    cleanup();
}
END_TEST

START_TEST(t_deleted_records_skipped)
{
    cleanup();
    auto de = new DE(100, 200, 2, 0, 1, true);

    size_t n = 500;
    for (uint64_t i = 0; i < n; i++)
        de->insert({i, (uint32_t)i}, false);

    /* tombstone every even key */
    for (uint64_t i = 0; i < n; i += 2)
        de->erase({i, (uint32_t)i}, nullptr, false);

    ck_assert_int_gt(de->get_height(), 0);

    std::vector<R> expected;
    for (uint64_t i = 1; i < n; i += 2)
        expected.push_back({i, (uint32_t)i});

    check_eq(scan(de, 0, n), expected);

    delete de;
    cleanup();
}
END_TEST

START_TEST(t_duplicate_keys)
{
    cleanup();
    auto de = new DE(100, 200, 2, 0, 1, true);

    /* first copy of each key goes out to the shards */
    size_t n = 300;
    for (uint64_t i = 0; i < n; i++)
        de->insert({i, 0}, false);

    ck_assert_int_gt(de->get_height(), 0);

    /* same keys with a different value, plus one exact duplicate */
    for (uint64_t i = 0; i < 50; i++)
        de->insert({i, 1}, false);
    de->insert({10, 0}, false);

    std::vector<R> expected;
    for (uint64_t i = 0; i < 50; i++)
    {
        expected.push_back({i, 0});
        if (i == 10)
            expected.push_back({i, 0});
        expected.push_back({i, 1});
    }

    check_eq(scan(de, 0, 49), expected);

    delete de;
    cleanup();
}
END_TEST

START_TEST(t_edge_ranges)
{
    cleanup();
    auto de = new DE(100, 200, 2, 0, 1, true);

    /* keys 100..1098 (even only), spread over shards and the buffer */
    for (uint64_t k = 100; k < 1100; k += 2)
        de->insert({k, (uint32_t)(k * 3)}, false);

    ck_assert_int_gt(de->get_height(), 0);

    /* inverted range */
    check_eq(scan(de, 500, 400), {});

    /* entirely below and entirely above the data */
    check_eq(scan(de, 0, 99), {});
    check_eq(scan(de, 1099, 5000), {});

    /* empty range between two existing keys */
    check_eq(scan(de, 501, 501), {});

    /* single key */
    check_eq(scan(de, 500, 500), {{500, 1500}});

    /* ranges hanging off either end of the data */
    check_eq(scan(de, 0, 104), {{100, 300}, {102, 306}, {104, 312}});
    check_eq(scan(de, 1094, 5000), {{1094, 3282}, {1096, 3288}, {1098, 3294}});

    delete de;
    cleanup();
}
END_TEST

Suite *unit_testing()
{
    Suite *unit = suite_create("Range Scan Testing");

    TCase *scan = tcase_create("de::DynamicExtension::range_scan Testing");
    tcase_add_test(scan, t_empty);
    tcase_add_test(scan, t_buffer_only);
    tcase_add_test(scan, t_shards_and_buffer);
    tcase_add_test(scan, t_deleted_records_skipped);
    tcase_add_test(scan, t_duplicate_keys);
    tcase_add_test(scan, t_edge_ranges);
    tcase_set_timeout(scan, 60);
    suite_add_tcase(unit, scan);

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
