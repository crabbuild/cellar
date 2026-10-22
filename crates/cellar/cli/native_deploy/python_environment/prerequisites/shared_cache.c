/* OS introspection only. No Python API, image constructor or application code. */
#include <stdbool.h>
#include <mach/mach.h>
#include <mach/task_info.h>
#include <mach-o/dyld_images.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

int cellar_python_shared_cache_uuid(uint8_t output[16]) {
    task_dyld_info_data_t info;
    mach_msg_type_number_t count = TASK_DYLD_INFO_COUNT;
    if (task_info(mach_task_self(), TASK_DYLD_INFO, (task_info_t)&info, &count) != KERN_SUCCESS ||
        count < TASK_DYLD_INFO_COUNT || info.all_image_info_format != TASK_DYLD_ALL_IMAGE_INFO_64 ||
        info.all_image_info_addr == 0 ||
        info.all_image_info_size < offsetof(struct dyld_all_image_infos, sharedCacheUUID) + 16) {
        return -1;
    }
    /* The kernel identifies our process's live dyld metadata. Its cache UUID
       is established before main and remains immutable for this process. */
    const struct dyld_all_image_infos *images = (const void *)(uintptr_t)info.all_image_info_addr;
    if (images->version < 13) {
        return -1;
    }
    memcpy(output, images->sharedCacheUUID, 16);
    return 0;
}

#ifdef CELLAR_PYTHON_SHARED_CACHE_TOOL
#include <stdio.h>
int main(void) {
    uint8_t uuid[16];
    if (cellar_python_shared_cache_uuid(uuid) != 0) {
        return 1;
    }
    for (unsigned int index = 0; index < 16; index++) {
        printf("%02x", uuid[index]);
    }
    putchar('\n');
    return 0;
}
#endif
