#include <cassert>

#include "include/testing.h"
#include "framework/structure/MutableBuffer.h"

#include <check.h>
#include <sys/stat.h>

#include <atomic>
#include <set>
#include <thread>
#include <utility>
#include <vector>

using namespace de;

#define WAL_PATH "wal.log"

/* mirrors the private WriteAheadLogger<R>::WAL_Entry layout */
struct TestWALEntry
{
    int type;
    Wrapped<Rec> payload;
};

enum
{
    T_INSERT,
    T_DELETE,
    T_FLUSH_B,
    T_FLUSH_C
};

static std::vector<TestWALEntry> read_wal()
{
    std::vector<TestWALEntry> entries;
    FILE *f = fopen(WAL_PATH, "rb");
    if (!f)
        return entries;

    TestWALEntry e;
    while (fread(&e, sizeof(e), 1, f) == 1)
        entries.push_back(e);
    fclose(f);
    return entries;
}

static off_t wal_size()
{
    struct stat st;
    if (stat(WAL_PATH, &st) < 0)
        return 0;
    return st.st_size;
}

START_TEST(replay)
{
    size_t lwm = 50, hwm = 100;

    auto buffer = new MutableBuffer<Rec>(lwm, hwm);

    for (size_t i = 0; i < lwm - 1; i++)
    {
        Rec r = {(u_int64_t)i, (u_int32_t)i};
        buffer->append(r);
    }

    delete buffer;

    auto buffer2 = new MutableBuffer<Rec>(lwm, hwm);
    auto bv = buffer2->get_buffer_view();

    ck_assert_int_eq(bv.get_record_count(), 49);

    for (size_t i = 0; i < lwm - 1; i++)
    {
        ck_assert_int_eq((bv.get(i))->rec.key, i);
        ck_assert_int_eq((bv.get(i))->rec.value, i);
    }

    remove(WAL_PATH);
}
END_TEST

START_TEST(concurrent_append)
{
    remove(WAL_PATH);
    const size_t thread_cnt = 8, per_thread = 250;

    {
        WriteAheadLogger<Rec> wal;
        std::vector<std::thread> threads;
        for (size_t t = 0; t < thread_cnt; t++)
        {
            threads.emplace_back([&wal, t, per_thread]()
                                 {
                for (size_t i = 0; i < per_thread; i++) {
                    Wrapped<Rec> w;
                    w.header = 0;
                    w.rec = {(uint64_t)t, (uint32_t)i};
                    wal.append(w);
                } });
        }
        for (auto &th : threads)
            th.join();
    }

    /* every append must occupy its own slot: no overwrites, no torn writes */
    ck_assert_int_eq(wal_size(), thread_cnt * per_thread * sizeof(TestWALEntry));

    auto entries = read_wal();
    ck_assert_int_eq(entries.size(), thread_cnt * per_thread);

    std::set<std::pair<uint64_t, uint32_t>> seen;
    std::vector<int64_t> last(thread_cnt, -1);
    for (auto &e : entries)
    {
        ck_assert_int_eq(e.type, T_INSERT);
        ck_assert_uint_lt(e.payload.rec.key, thread_cnt);
        ck_assert_uint_lt(e.payload.rec.value, per_thread);
        ck_assert(seen.insert({e.payload.rec.key, e.payload.rec.value}).second);

        /* per-thread program order must be preserved in the log */
        ck_assert_int_lt(last[e.payload.rec.key], (int64_t)e.payload.rec.value);
        last[e.payload.rec.key] = e.payload.rec.value;
    }
    ck_assert_int_eq(seen.size(), thread_cnt * per_thread);

    remove(WAL_PATH);
}
END_TEST

START_TEST(concurrent_append_remove)
{
    remove(WAL_PATH);
    const size_t thread_cnt = 8, per_thread = 200;

    {
        WriteAheadLogger<Rec> wal;
        std::vector<std::thread> threads;
        for (size_t t = 0; t < thread_cnt; t++)
        {
            threads.emplace_back([&wal, t, per_thread]()
                                 {
                for (size_t i = 0; i < per_thread; i++) {
                    Wrapped<Rec> w;
                    w.header = 0;
                    w.rec = {(uint64_t)t, (uint32_t)i};
                    if (t % 2 == 0)
                        wal.append(w);
                    else
                        wal.remove(w);
                } });
        }
        for (auto &th : threads)
            th.join();
    }

    auto entries = read_wal();
    ck_assert_int_eq(entries.size(), thread_cnt * per_thread);

    size_t inserts = 0, deletes = 0;
    for (auto &e : entries)
    {
        if (e.type == T_INSERT)
        {
            ck_assert_int_eq(e.payload.rec.key % 2, 0);
            inserts++;
        }
        else
        {
            ck_assert_int_eq(e.type, T_DELETE);
            ck_assert_int_eq(e.payload.rec.key % 2, 1);
            deletes++;
        }
    }
    ck_assert_int_eq(inserts, thread_cnt / 2 * per_thread);
    ck_assert_int_eq(deletes, thread_cnt / 2 * per_thread);

    remove(WAL_PATH);
}
END_TEST

/*
 * Writers append while a flusher repeatedly runs flush_begin/flush_complete.
 * Every entry written after the final flush_begin must survive, the file must
 * never contain partial entries, and no FLUSH markers may remain after
 * flush_complete.
 */
START_TEST(concurrent_append_with_flush)
{
    remove(WAL_PATH);
    const size_t thread_cnt = 4, per_thread = 200, flush_cnt = 20;

    std::atomic<bool> done = false;
    {
        WriteAheadLogger<Rec> wal;
        std::vector<std::thread> threads;
        for (size_t t = 0; t < thread_cnt; t++)
        {
            threads.emplace_back([&wal, t, per_thread]()
                                 {
                for (size_t i = 0; i < per_thread; i++) {
                    Wrapped<Rec> w;
                    w.header = 0;
                    w.rec = {(uint64_t)t, (uint32_t)i};
                    wal.append(w);
                } });
        }

        std::thread flusher([&wal, &done, flush_cnt]()
                            {
            for (size_t i = 0; i < flush_cnt && !done.load(); i++) {
                wal.flush_begin();
                std::this_thread::yield();
                wal.flush_complete();
                ck_assert_int_eq(wal_size() % sizeof(TestWALEntry), 0);
            } });

        for (auto &th : threads)
            th.join();
        done.store(true);
        flusher.join();

        /* a final flush with no concurrent writers must empty the log */
        wal.flush_begin();
        wal.flush_complete();
        ck_assert_int_eq(wal_size(), 0);

        /* and the logger must still be usable afterwards */
        Wrapped<Rec> w;
        w.header = 0;
        w.rec = {42, 42};
        wal.append(w);
    }

    auto entries = read_wal();
    ck_assert_int_eq(entries.size(), 1);
    ck_assert_int_eq(entries[0].type, T_INSERT);
    ck_assert_int_eq(entries[0].payload.rec.key, 42);

    remove(WAL_PATH);
}
END_TEST

START_TEST(concurrent_buffer_replay)
{
    remove(WAL_PATH);
    const size_t thread_cnt = 4, per_thread = 100;
    size_t lwm = 500, hwm = 1000;

    auto buffer = new MutableBuffer<Rec>(lwm, hwm);
    std::vector<std::thread> threads;
    for (size_t t = 0; t < thread_cnt; t++)
    {
        threads.emplace_back([buffer, t, per_thread]()
                             {
            for (size_t i = 0; i < per_thread; i++) {
                Rec r = {(uint64_t)(t * per_thread + i), (uint32_t)i};
                ck_assert_int_eq(buffer->append(r), 1);
            } });
    }
    for (auto &th : threads)
        th.join();
    delete buffer;

    auto buffer2 = new MutableBuffer<Rec>(lwm, hwm);
    {
        auto bv = buffer2->get_buffer_view();
        ck_assert_int_eq(bv.get_record_count(), thread_cnt * per_thread);

        std::set<uint64_t> keys;
        for (size_t i = 0; i < bv.get_record_count(); i++)
            keys.insert(bv.get(i)->rec.key);
        ck_assert_int_eq(keys.size(), thread_cnt * per_thread);
    }
    delete buffer2;

    remove(WAL_PATH);
}
END_TEST

Suite *unit_testing()
{
    Suite *unit = suite_create("Write Ahead Log Unit Testing");
    TCase *test = tcase_create("de::Write Ahead Log Replay Testing");
    tcase_add_test(test, replay);
    suite_add_tcase(unit, test);

    TCase *conc = tcase_create("de::Write Ahead Log Concurrency Testing");
    tcase_add_test(conc, concurrent_append);
    tcase_add_test(conc, concurrent_append_remove);
    tcase_add_test(conc, concurrent_append_with_flush);
    tcase_add_test(conc, concurrent_buffer_replay);
    tcase_set_timeout(conc, 120);
    suite_add_tcase(unit, conc);
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
