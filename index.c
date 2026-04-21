// index.c — Staging area implementation
//
// Text format of .pes/index (one entry per line, sorted by path):
//
//   <mode-octal> <64-char-hex-hash> <mtime-seconds> <size> <path>
//
// Example:
//   100644 a1b2c3d4e5f6...  1699900000 42 README.md
//   100644 f7e8d9c0b1a2...  1699900100 128 src/main.c
//
// This is intentionally a simple text format. No magic numbers, no
// binary parsing. The focus is on the staging area CONCEPT (tracking
// what will go into the next commit) and ATOMIC WRITES (temp+rename).
//
// PROVIDED functions: index_find, index_remove, index_status
// TODO functions:     index_load, index_save, index_add

#include "index.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>
#include <errno.h>
#include <stdint.h>

// object.c functions (declare here in case pes.h does not expose them)
int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out);
void hash_to_hex(const ObjectID *id, char *hex_out);
int hex_to_hash(const char *hex, ObjectID *id_out);

// ─── PROVIDED ────────────────────────────────────────────────────────────────

// Find an index entry by path (linear scan).
IndexEntry* index_find(Index *index, const char *path) {
    for (int i = 0; i < index->count; i++) {
        if (strcmp(index->entries[i].path, path) == 0)
            return &index->entries[i];
    }
    return NULL;
}

// Remove a file from the index.
// Returns 0 on success, -1 if path not in index.
int index_remove(Index *index, const char *path) {
    for (int i = 0; i < index->count; i++) {
        if (strcmp(index->entries[i].path, path) == 0) {
            int remaining = index->count - i - 1;
            if (remaining > 0)
                memmove(&index->entries[i], &index->entries[i + 1],
                        remaining * sizeof(IndexEntry));
            index->count--;
            return index_save(index);
        }
    }
    fprintf(stderr, "error: '%s' is not in the index\n", path);
    return -1;
}

// Print the status of the working directory.
//
// Identifies files that are staged, unstaged (modified/deleted in working dir),
// and untracked (present in working dir but not in index).
// Returns 0.
int index_status(const Index *index) {
    printf("Staged changes:\n");
    int staged_count = 0;
    // Note: A true Git implementation deeply diffs against the HEAD tree here.
    // For this lab, displaying indexed files represents the staging intent.
    for (int i = 0; i < index->count; i++) {
        printf("  staged:     %s\n", index->entries[i].path);
        staged_count++;
    }
    if (staged_count == 0) printf("  (nothing to show)\n");
    printf("\n");

    printf("Unstaged changes:\n");
    int unstaged_count = 0;
    for (int i = 0; i < index->count; i++) {
        struct stat st;
        if (stat(index->entries[i].path, &st) != 0) {
            printf("  deleted:    %s\n", index->entries[i].path);
            unstaged_count++;
        } else {
            // Fast diff: check metadata instead of re-hashing file content
            if (st.st_mtime != (time_t)index->entries[i].mtime_sec ||
                st.st_size  != (off_t)index->entries[i].size) {
                printf("  modified:   %s\n", index->entries[i].path);
                unstaged_count++;
            }
        }
    }
    if (unstaged_count == 0) printf("  (nothing to show)\n");
    printf("\n");

    printf("Untracked files:\n");
    int untracked_count = 0;
    DIR *dir = opendir(".");
    if (dir) {
        struct dirent *ent;
        while ((ent = readdir(dir)) != NULL) {
            // Skip hidden directories, parent directories, and build artifacts
            if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0) continue;
            if (strcmp(ent->d_name, ".pes") == 0) continue;
            if (strcmp(ent->d_name, "pes") == 0) continue; // compiled executable
            if (strstr(ent->d_name, ".o") != NULL) continue; // object files

            // Check if file is tracked in the index
            int is_tracked = 0;
            for (int i = 0; i < index->count; i++) {
                if (strcmp(index->entries[i].path, ent->d_name) == 0) {
                    is_tracked = 1;
                    break;
                }
            }

            if (!is_tracked) {
                struct stat st;
                stat(ent->d_name, &st);
                if (S_ISREG(st.st_mode)) { // Only list regular files for simplicity
                    printf("  untracked:  %s\n", ent->d_name);
                    untracked_count++;
                }
            }
        }
        closedir(dir);
    }
    if (untracked_count == 0) printf("  (nothing to show)\n");
    printf("\n");

    return 0;
}

// ─── Helpers ────────────────────────────────────────────────────────────────

static int compare_index_entries(const void *a, const void *b) {
    const IndexEntry *ea = (const IndexEntry *)a;
    const IndexEntry *eb = (const IndexEntry *)b;
    return strcmp(ea->path, eb->path);
}

// ─── TODO: Implement these ───────────────────────────────────────────────────

// Load the index from .pes/index.
//
// Returns 0 on success, -1 on error.
int index_load(Index *index) {
    if (!index) return -1;

    index->count = 0;

    FILE *fp = fopen(INDEX_FILE, "r");
    if (!fp) {
        if (errno == ENOENT) {
            return 0; // no index yet = empty index
        }
        return -1;
    }

    char line[2048];

    while (fgets(line, sizeof(line), fp) != NULL) {
        if (index->count >= MAX_INDEX_ENTRIES) {
            fclose(fp);
            return -1;
        }

        IndexEntry entry;
        memset(&entry, 0, sizeof(entry));

        char hash_hex[HASH_HEX_SIZE + 1];
        char path_buf[512];
        unsigned int mode_tmp;
        unsigned long long mtime_tmp;
        unsigned int size_tmp;

        int matched = sscanf(line, "%o %64s %llu %u %511[^\n]",
                             &mode_tmp, hash_hex, &mtime_tmp, &size_tmp, path_buf);
        if (matched != 5) {
            fclose(fp);
            return -1;
        }

        entry.mode = (uint32_t)mode_tmp;
        entry.mtime_sec = (uint64_t)mtime_tmp;
        entry.size = (uint32_t)size_tmp;

        if (hex_to_hash(hash_hex, &entry.hash) != 0) {
            fclose(fp);
            return -1;
        }

        strncpy(entry.path, path_buf, sizeof(entry.path) - 1);
        entry.path[sizeof(entry.path) - 1] = '\0';

        index->entries[index->count++] = entry;
    }

    if (ferror(fp)) {
        fclose(fp);
        return -1;
    }

    fclose(fp);
    return 0;
}

// Save the index to .pes/index atomically.
//
// Returns 0 on success, -1 on error.

int index_save(const Index *index) {
    if (!index) return -1;

    /* Make a heap copy so we can sort without mutating the caller's const object */
    Index *sorted = malloc(sizeof(Index));
    if (!sorted) return -1;
    *sorted = *index;

    qsort(sorted->entries, sorted->count, sizeof(IndexEntry), compare_index_entries);

    char tmp_path[512];
    int n = snprintf(tmp_path, sizeof(tmp_path), "%s.tmp", INDEX_FILE);
    if (n < 0 || (size_t)n >= sizeof(tmp_path)) {
        free(sorted);
        return -1;
    }

    FILE *fp = fopen(tmp_path, "w");
    if (!fp) {
        free(sorted);
        return -1;
    }

    for (int i = 0; i < sorted->count; i++) {
        char hex[HASH_HEX_SIZE + 1];
        hash_to_hex(&sorted->entries[i].hash, hex);

        if (fprintf(fp, "%o %s %llu %u %s\n",
                    sorted->entries[i].mode,
                    hex,
                    (unsigned long long)sorted->entries[i].mtime_sec,
                    sorted->entries[i].size,
                    sorted->entries[i].path) < 0) {
            fclose(fp);
            unlink(tmp_path);
            free(sorted);
            return -1;
        }
    }

    if (fflush(fp) != 0) {
        fclose(fp);
        unlink(tmp_path);
        free(sorted);
        return -1;
    }

    if (fsync(fileno(fp)) != 0) {
        fclose(fp);
        unlink(tmp_path);
        free(sorted);
        return -1;
    }

    if (fclose(fp) != 0) {
        unlink(tmp_path);
        free(sorted);
        return -1;
    }

    if (rename(tmp_path, INDEX_FILE) != 0) {
        unlink(tmp_path);
        free(sorted);
        return -1;
    }

    free(sorted);
    return 0;
}
 

// Stage a file for the next commit.
//
// Returns 0 on success, -1 on error.
int index_add(Index *index, const char *path) {
    if (!index || !path) {
        fprintf(stderr, "index_add: invalid arguments\n");
        return -1;
    }

    struct stat st;
    if (stat(path, &st) != 0) {
        perror("index_add: stat");
        return -1;
    }

    if (!S_ISREG(st.st_mode)) {
        fprintf(stderr, "index_add: '%s' is not a regular file\n", path);
        return -1;
    }

    FILE *fp = fopen(path, "rb");
    if (!fp) {
        perror("index_add: fopen");
        return -1;
    }

    size_t file_size = (size_t)st.st_size;
    void *data = malloc(file_size > 0 ? file_size : 1);
    if (!data) {
        fprintf(stderr, "index_add: malloc failed\n");
        fclose(fp);
        return -1;
    }

    if (file_size > 0) {
        size_t got = fread(data, 1, file_size, fp);
        if (got != file_size) {
            fprintf(stderr, "index_add: fread failed\n");
            free(data);
            fclose(fp);
            return -1;
        }
    }

    fclose(fp);

    ObjectID blob_id;
    if (object_write(OBJ_BLOB, data, file_size, &blob_id) != 0) {
        fprintf(stderr, "index_add: object_write failed\n");
        free(data);
        return -1;
    }

    free(data);

    IndexEntry *entry = index_find(index, path);
    if (!entry) {
        if (index->count >= MAX_INDEX_ENTRIES) {
            fprintf(stderr, "index_add: index is full\n");
            return -1;
        }
        entry = &index->entries[index->count++];
        memset(entry, 0, sizeof(*entry));
    }

    entry->mode = (uint32_t)st.st_mode;
    entry->hash = blob_id;
    entry->mtime_sec = (uint64_t)st.st_mtime;
    entry->size = (uint32_t)st.st_size;

    strncpy(entry->path, path, sizeof(entry->path) - 1);
    entry->path[sizeof(entry->path) - 1] = '\0';

    if (index_save(index) != 0) {
        fprintf(stderr, "index_add: index_save failed\n");
        return -1;
    }

    return 0;
}

 
