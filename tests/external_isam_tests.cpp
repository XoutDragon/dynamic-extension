/*
clang++ -std=c++20 -Iinclude -Itests/include -Iexternal/psudb-common/cpp/include tests/external_isam_tests.cpp -Iexternal/stduuid/include -o bin/external_isam_tests -lgsl -lgslcblas -latomic -ggdb -lcheck -lsubunit
*/

#include <dirent.h>
#include <vector>

#include "shard/ExternalISAMTree.h"
#include "query/pointlookup.h"
#include "testing.h"

#include <cassert>
#include <check.h>

using namespace de;

typedef Rec R;
typedef ISAMTree<R> Shard;
typedef pl::Query<Shard> Q;

#define SHARD_DIR "shards/"
#define TEMP_SHARD_DIR "tmp_shards/"
#define CHECKPOINT_PATH "checkpoint.json"

void cleanup()
{
    DIR *dir = opendir(SHARD_DIR);

    struct dirent *entry;
    while ((entry = readdir(dir)) != NULL)
    {
        if (dir == NULL)
            return;
        std::string filename = std::string(SHARD_DIR) + entry->d_name;
        remove(filename.c_str());
    }

    rmdir(TEMP_SHARD_DIR);
    rmdir(SHARD_DIR);
    remove(CHECKPOINT_PATH);
    remove(WAL_PATH);
}

START_TEST(t_mbuffer_init)
{
    size_t n = 24;
    auto buffer = new MutableBuffer<R>(n / 2, n);

    for (u_int32_t i = 0; i < n; i++)
    {
        buffer->append({(u_int32_t)i, (u_int32_t)i});
    }

    Shard *shard = new Shard(buffer->get_buffer_view());
    ck_assert_uint_eq(shard->get_record_count(), n);

    delete buffer;
    delete shard;
}

START_TEST(t_wss_init)
{
    size_t n = 512;
    auto mbuffer1 = create_test_mbuffer<R>(n);
    auto mbuffer2 = create_test_mbuffer<R>(n);
    auto mbuffer3 = create_test_mbuffer<R>(n);

    auto shard1 = new Shard(mbuffer1->get_buffer_view());
    auto shard2 = new Shard(mbuffer2->get_buffer_view());
    auto shard3 = new Shard(mbuffer3->get_buffer_view());

    std::vector<Shard *> shards = {shard1, shard2, shard3};
    auto shard4 = new Shard(shards);

    ck_assert_int_eq(shard4->get_record_count(), n * 3);
    ck_assert_int_eq(shard4->get_tombstone_count(), 0);

    delete mbuffer1;
    delete mbuffer2;
    delete mbuffer3;

    delete shard1;
    delete shard2;
    delete shard3;
    delete shard4;
}

START_TEST(t_point_lookup)
{
    size_t n = 16;

    auto buffer = create_sequential_mbuffer<R>(0, n);
    auto wss = Shard(buffer->get_buffer_view());

    std::byte *rec_buffer = nullptr;

    for (int i = 0; i < n; i++)
    {
        R rec = {(u_int64_t)i, (u_int32_t)i};
        auto result = wss.point_lookup(rec, false, rec_buffer);

        ck_assert_ptr_nonnull(result);
        ck_assert_int_eq(result->rec.key, rec.key);
        ck_assert_int_eq(result->rec.value, rec.value);
    }

    free(rec_buffer);
    delete buffer;
}

START_TEST(t_point_lookup_miss)
{
    size_t n = 1000;

    auto buffer = create_sequential_mbuffer<R>(0, n);
    auto wss = Shard(buffer->get_buffer_view());

    for (size_t i = n + 100; i < 2 * n; i++)
    {
        R r;
        r.key = i;
        r.value = i;

        auto result = wss.point_lookup(r);
        ck_assert_ptr_null(result);
    }

    delete buffer;
}
END_TEST

Suite *unit_testing()
{
    Suite *unit = suite_create("ISAMTree Shard Unit Testing");

    TCase *create = tcase_create("de::ISAMTree constructor Testing");
    tcase_add_test(create, t_mbuffer_init);
    tcase_add_test(create, t_wss_init);
    tcase_set_timeout(create, 100);
    suite_add_tcase(unit, create);

    TCase *lookup = tcase_create("de:ISAMTree:point_lookup Testing");
    tcase_add_test(lookup, t_point_lookup);
    tcase_add_test(lookup, t_point_lookup_miss);
    suite_add_tcase(unit, lookup);

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
    cleanup();
    return (unit_failed == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
}
