// tree.c — Tree object serialization and construction
//
// PROVIDED functions: get_file_mode, tree_parse, tree_serialize
// TODO functions:     tree_from_index
//
// Binary tree format (per entry, concatenated with no separators):
//   "<mode-as-ascii-octal> <name>\0<32-byte-binary-hash>"
//
// Example single entry (conceptual):
//   "100644 hello.txt\0" followed by 32 raw bytes of SHA-256

#include "tree.h"
#include "index.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <sys/stat.h>
#include <stdint.h>

// object_write is implemented in object.c.
// Declare it here in case pes.h does not already expose the prototype.
int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out);

// ─── Mode Constants ─────────────────────────────────────────────────────────

#define MODE_FILE      0100644
#define MODE_EXEC      0100755
#define MODE_DIR       0040000

// ─── PROVIDED ───────────────────────────────────────────────────────────────

// Determine the object mode for a filesystem path.
uint32_t get_file_mode(const char *path) {
    struct stat st;
    if (lstat(path, &st) != 0) return 0;

    if (S_ISDIR(st.st_mode))  return MODE_DIR;
    if (st.st_mode & S_IXUSR) return MODE_EXEC;
    return MODE_FILE;
}

// Parse binary tree data into a Tree struct safely.
// Returns 0 on success, -1 on parse error.
int tree_parse(const void *data, size_t len, Tree *tree_out) {
    tree_out->count = 0;
    const uint8_t *ptr = (const uint8_t *)data;
    const uint8_t *end = ptr + len;

    while (ptr < end && tree_out->count < MAX_TREE_ENTRIES) {
        TreeEntry *entry = &tree_out->entries[tree_out->count];

        // 1. Safely find the space character for the mode
        const uint8_t *space = memchr(ptr, ' ', end - ptr);
        if (!space) return -1; // Malformed data

        // Parse mode into an isolated buffer
        char mode_str[16] = {0};
        size_t mode_len = (size_t)(space - ptr);
        if (mode_len >= sizeof(mode_str)) return -1;
        memcpy(mode_str, ptr, mode_len);
        entry->mode = (uint32_t)strtol(mode_str, NULL, 8);

        ptr = space + 1; // Skip space

        // 2. Safely find the null terminator for the name
        const uint8_t *null_byte = memchr(ptr, '\0', end - ptr);
        if (!null_byte) return -1; // Malformed data

        size_t name_len = (size_t)(null_byte - ptr);
        if (name_len >= sizeof(entry->name)) return -1;
        memcpy(entry->name, ptr, name_len);
        entry->name[name_len] = '\0'; // Ensure null-terminated

        ptr = null_byte + 1; // Skip null byte

        // 3. Read the 32-byte binary hash
        if (ptr + HASH_SIZE > end) return -1;
        memcpy(entry->hash.hash, ptr, HASH_SIZE);
        ptr += HASH_SIZE;

        tree_out->count++;
    }

    if (ptr != end) return -1;
    return 0;
}

// Helper for qsort to ensure consistent tree hashing
static int compare_tree_entries(const void *a, const void *b) {
    return strcmp(((const TreeEntry *)a)->name, ((const TreeEntry *)b)->name);
}

// Serialize a Tree struct into binary format for storage.
// Caller must free(*data_out).
// Returns 0 on success, -1 on error.
int tree_serialize(const Tree *tree, void **data_out, size_t *len_out) {
    if (!tree || !data_out || !len_out) return -1;

    // Allow empty trees too.
    size_t max_size = (tree->count > 0) ? ((size_t)tree->count * 296u) : 1u;
    uint8_t *buffer = malloc(max_size);
    if (!buffer) return -1;

    // Create a mutable copy to sort entries (Git requirement)
    Tree sorted_tree = *tree;
    qsort(sorted_tree.entries, sorted_tree.count, sizeof(TreeEntry), compare_tree_entries);

    size_t offset = 0;
    for (int i = 0; i < sorted_tree.count; i++) {
        const TreeEntry *entry = &sorted_tree.entries[i];

        int written = sprintf((char *)buffer + offset, "%o %s", entry->mode, entry->name);
        if (written < 0) {
            free(buffer);
            return -1;
        }

        offset += (size_t)written + 1; // +1 for '\0'
        memcpy(buffer + offset, entry->hash.hash, HASH_SIZE);
        offset += HASH_SIZE;
    }

    *data_out = buffer;
    *len_out = offset;
    return 0;
}

// ─── Helpers for recursive tree construction ────────────────────────────────

static int starts_with_prefix(const char *path, const char *prefix) {
    size_t plen = strlen(prefix);
    return strncmp(path, prefix, plen) == 0;
}

static int dir_already_seen(char seen[][256], int seen_count, const char *name) {
    for (int i = 0; i < seen_count; i++) {
        if (strcmp(seen[i], name) == 0) return 1;
    }
    return 0;
}

static int write_tree_level(const Index *index, const char *prefix, ObjectID *id_out) {
    if (!index || !prefix || !id_out) return -1;

    Tree tree;
    tree.count = 0;

    char seen_dirs[MAX_TREE_ENTRIES][256];
    int seen_dir_count = 0;

    size_t prefix_len = strlen(prefix);

    for (int i = 0; i < index->count; i++) {
        const IndexEntry *ie = &index->entries[i];

        if (!starts_with_prefix(ie->path, prefix)) {
            continue;
        }

        const char *rest = ie->path + prefix_len;
        if (*rest == '\0') {
            continue;
        }

        const char *slash = strchr(rest, '/');

        if (slash == NULL) {
            // Direct file in this directory level
            if (tree.count >= MAX_TREE_ENTRIES) return -1;

            TreeEntry *te = &tree.entries[tree.count++];
            te->mode = ie->mode;
            te->hash = ie->hash;

            if (strlen(rest) >= sizeof(te->name)) return -1;
            strcpy(te->name, rest);
        } else {
            // Subdirectory entry: recurse once per unique directory name
            size_t dir_len = (size_t)(slash - rest);
            if (dir_len == 0 || dir_len >= 256) return -1;

            char dirname[256];
            memcpy(dirname, rest, dir_len);
            dirname[dir_len] = '\0';

            if (dir_already_seen(seen_dirs, seen_dir_count, dirname)) {
                continue;
            }

            if (seen_dir_count >= MAX_TREE_ENTRIES) return -1;
            strcpy(seen_dirs[seen_dir_count++], dirname);

            char child_prefix[1024];
            int n = snprintf(child_prefix, sizeof(child_prefix), "%s%s/", prefix, dirname);
            if (n < 0 || (size_t)n >= sizeof(child_prefix)) return -1;

            ObjectID child_id;
            if (write_tree_level(index, child_prefix, &child_id) != 0) {
                return -1;
            }

            if (tree.count >= MAX_TREE_ENTRIES) return -1;

            TreeEntry *te = &tree.entries[tree.count++];
            te->mode = MODE_DIR;
            te->hash = child_id;
            strcpy(te->name, dirname);
        }
    }

    void *data = NULL;
    size_t len = 0;

    if (tree_serialize(&tree, &data, &len) != 0) {
        return -1;
    }

    int rc = object_write(OBJ_TREE, data, len, id_out);
    free(data);
    return rc;
}

// ─── TODO: Implement these ──────────────────────────────────────────────────

// Build a tree hierarchy from the current index and write all tree
// objects to the object store.
//
// Returns 0 on success, -1 on error.
int tree_from_index(ObjectID *id_out) {
    if (!id_out) return -1;

    Index index;
    if (index_load(&index) != 0) {
        return -1;
    }

    return write_tree_level(&index, "", id_out);
}
