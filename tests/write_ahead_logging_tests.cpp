#include <cassert>

#include "include/testing.h"
#include "framework/structure/MutableBuffer.h"

#include <check.h>
using namespace de;

#define WAL_PATH "wal.log"

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

Suite *unit_testing()
{
    Suite *unit = suite_create("Write Ahead Log Unit Testing");
    TCase *test = tcase_create("de::Write Ahead Log Replay Testing");
    tcase_add_test(test, replay);
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
