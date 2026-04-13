
#include <check.h>
#include <dirent.h>
#include <fcntl.h>

#include <cassert>

#include "framework/DynamicExtension.h"
#include "include/testing.h"
#include "query/rangequery.h"
#include "shard/ISAMTree.h"

using namespace de;

typedef Rec R;
typedef ISAMTree<R> S;
typedef rq::Query<S> Q;

#define SHARD_DIR "shards/"
#define TEMP_SHARD_DIR "tmp_shards/"

START_TEST(rebuild) {
    size_t buffer_low_watermark = 50;
    size_t buffer_high_watermark = 100;
    size_t scale_factor = 3;

    auto* de = new DynamicExtension<S, Q>(buffer_low_watermark, buffer_high_watermark, scale_factor);

    for (size_t i = 0; i < buffer_low_watermark * 30; i++) {
        R rec;
        rec.key = i * 100;
        rec.value = i * 100;
        de->insert(rec);
    }

    int reccnt = de->get_record_count();

    delete de;

    auto* de2 = new DynamicExtension<S, Q>(buffer_low_watermark, buffer_high_watermark, scale_factor);

    ck_assert_int_eq(reccnt, de2->get_record_count());

    DIR* dir = opendir(SHARD_DIR);

    struct dirent* entry;
    while ((entry = readdir(dir)) != NULL) {
        if (dir == NULL)
            return;
        std::string filename = std::string(SHARD_DIR) + entry->d_name;
        remove(filename.c_str());
    }

    // rmdir(TEMP_SHARD_DIR);
    // rmdir(SHARD_DIR);
    // remove(CHECKPOINT_PATH);
    // remove(WAL_PATH);
}
END_TEST

Suite* unit_testing() {
    Suite* unit = suite_create("Checkpointing Unit Testing");
    TCase* test = tcase_create("de::System Rebuild from Checkpoint Testing");
    tcase_add_test(test, rebuild);
    suite_add_tcase(unit, test);
    return unit;
}

int shard_unit_tests() {
    int failed = 0;
    Suite* unit = unit_testing();
    SRunner* unit_shardner = srunner_create(unit);

    srunner_run_all(unit_shardner, CK_NORMAL);
    failed = srunner_ntests_failed(unit_shardner);
    srunner_free(unit_shardner);

    return failed;
}

int main() {
    int unit_failed = shard_unit_tests();

    return (unit_failed == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
}
