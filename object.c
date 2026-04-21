// object.c — Content-addressable object store
//
// Every piece of data (file contents, directory listings, commits) is stored
// as an "object" named by its SHA-256 hash. Objects are stored under
// .pes/objects/XX/YYYYYY... where XX is the first two hex characters of the
// hash (directory sharding).
//
// PROVIDED functions: compute_hash, object_path, object_exists, hash_to_hex, hex_to_hash
// TODO functions:     object_write, object_read

#include "pes.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <openssl/evp.h>
#include <errno.h>

// ─── PROVIDED ────────────────────────────────────────────────────────────────

void hash_to_hex(const ObjectID *id, char *hex_out) {
    for (int i = 0; i < HASH_SIZE; i++) {
        sprintf(hex_out + i * 2, "%02x", id->hash[i]);
    }
    hex_out[HASH_HEX_SIZE] = '\0';
}

int hex_to_hash(const char *hex, ObjectID *id_out) {
    if (strlen(hex) < HASH_HEX_SIZE) return -1;
    for (int i = 0; i < HASH_SIZE; i++) {
        unsigned int byte;
        if (sscanf(hex + i * 2, "%2x", &byte) != 1) return -1;
        id_out->hash[i] = (uint8_t)byte;
    }
    return 0;
}

void compute_hash(const void *data, size_t len, ObjectID *id_out) {
    unsigned int hash_len;
    EVP_MD_CTX *ctx = EVP_MD_CTX_new();
    EVP_DigestInit_ex(ctx, EVP_sha256(), NULL);
    EVP_DigestUpdate(ctx, data, len);
    EVP_DigestFinal_ex(ctx, id_out->hash, &hash_len);
    EVP_MD_CTX_free(ctx);
}

// Get the filesystem path where an object should be stored.
// Format: .pes/objects/XX/YYYYYYYY...
// The first 2 hex chars form the shard directory; the rest is the filename.
void object_path(const ObjectID *id, char *path_out, size_t path_size) {
    char hex[HASH_HEX_SIZE + 1];
    hash_to_hex(id, hex);
    snprintf(path_out, path_size, "%s/%.2s/%s", OBJECTS_DIR, hex, hex + 2);
}

int object_exists(const ObjectID *id) {
    char path[512];
    object_path(id, path, sizeof(path));
    return access(path, F_OK) == 0;
}

// ─── TODO: Implement these ──────────────────────────────────────────────────

static const char *object_type_name(ObjectType type) {
    switch (type) {
        case OBJ_BLOB:   return "blob";
        case OBJ_TREE:   return "tree";
        case OBJ_COMMIT: return "commit";
        default:         return NULL;
    }
}

// Write an object to the store.
//
// Object format on disk:
//   "<type> <size>\0<data>"
//   where <type> is "blob", "tree", or "commit"
//   and <size> is the decimal string of the data length
//
// Returns 0 on success, -1 on error.

static const char *object_type_name(ObjectType type) {
    switch (type) {
        case OBJ_BLOB:   return "blob";
        case OBJ_TREE:   return "tree";
        case OBJ_COMMIT: return "commit";
        default:         return NULL;
    }
}


int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out) {
    const char *type_name = object_type_name(type);
<<<<<<< HEAD
    if (!type_name || !data || !id_out) return -1;
=======
    if (!type_name || !id_out) return -1;
    if (len > 0 && data == NULL) return -1;
>>>>>>> 3dab50f (Complete PES VCS implementation)

    char header[64];
    int header_len = snprintf(header, sizeof(header), "%s %zu", type_name, len);
    if (header_len < 0 || (size_t)header_len + 1 > sizeof(header)) return -1;
<<<<<<< HEAD
    header_len += 1;   // include '\0'

    size_t full_len = (size_t)header_len + len;
    uint8_t *full_obj = malloc(full_len);
    if (!full_obj) return -1;

    memcpy(full_obj, header, (size_t)header_len);
    memcpy(full_obj + header_len, data, len);
=======
    header_len += 1;  // include '\0'

    size_t full_len = (size_t)header_len + len;
    unsigned char *full_obj = malloc(full_len);
    if (!full_obj) return -1;

    memcpy(full_obj, header, (size_t)header_len);
    if (len > 0) {
        memcpy(full_obj + header_len, data, len);
    }
>>>>>>> 3dab50f (Complete PES VCS implementation)

    ObjectID id;
    compute_hash(full_obj, full_len, &id);

    if (object_exists(&id)) {
        *id_out = id;
        free(full_obj);
        return 0;
    }

    char final_path[512];
    object_path(&id, final_path, sizeof(final_path));

    char hex[HASH_HEX_SIZE + 1];
    hash_to_hex(&id, hex);

    char shard_dir[512];
<<<<<<< HEAD
    snprintf(shard_dir, sizeof(shard_dir), "%s/%.2s", OBJECTS_DIR, hex);
=======
    int dir_len = snprintf(shard_dir, sizeof(shard_dir), "%s/%.2s", OBJECTS_DIR, hex);
    if (dir_len < 0 || (size_t)dir_len >= sizeof(shard_dir)) {
        free(full_obj);
        return -1;
    }
>>>>>>> 3dab50f (Complete PES VCS implementation)

    if (mkdir(shard_dir, 0755) != 0 && errno != EEXIST) {
        free(full_obj);
        return -1;
    }

    char temp_path[512];
<<<<<<< HEAD
    snprintf(temp_path, sizeof(temp_path), "%s/tmp-%ld-%d", shard_dir, (long)getpid(), rand());
=======
    int tmp_len = snprintf(temp_path, sizeof(temp_path),
                           "%s/tmp-%ld-%d", shard_dir, (long)getpid(), rand());
    if (tmp_len < 0 || (size_t)tmp_len >= sizeof(temp_path)) {
        free(full_obj);
        return -1;
    }
>>>>>>> 3dab50f (Complete PES VCS implementation)

    int fd = open(temp_path, O_CREAT | O_WRONLY | O_TRUNC, 0644);
    if (fd < 0) {
        free(full_obj);
        return -1;
    }

    size_t written_total = 0;
    while (written_total < full_len) {
        ssize_t n = write(fd, full_obj + written_total, full_len - written_total);
        if (n < 0) {
            close(fd);
            unlink(temp_path);
            free(full_obj);
            return -1;
        }
        written_total += (size_t)n;
    }

    if (fsync(fd) != 0) {
        close(fd);
        unlink(temp_path);
        free(full_obj);
        return -1;
    }

    if (close(fd) != 0) {
        unlink(temp_path);
        free(full_obj);
        return -1;
    }

    if (rename(temp_path, final_path) != 0) {
        unlink(temp_path);
        free(full_obj);
        return -1;
    }

    int dirfd = open(shard_dir, O_RDONLY | O_DIRECTORY);
    if (dirfd >= 0) {
        fsync(dirfd);
        close(dirfd);
    }

    *id_out = id;
    free(full_obj);
    return 0;
}

// Read an object from the store.
//
// Steps:
//   1. Build the file path from the hash using object_path()
//   2. Open and read the entire file
//   3. Parse the header to extract the type string and size
//   4. Verify integrity: recompute the SHA-256 of the file contents
//      and compare to the expected hash (from *id). Return -1 if mismatch.
//   5. Set *type_out to the parsed ObjectType
//   6. Allocate a buffer, copy the data portion (after the \0), set *data_out and *len_out
//
// The caller is responsible for calling free(*data_out).
<<<<<<< HEAD
// Returns 0 on success, -1 on error (file not found, corrupt, etc.).

=======
// Returns 0 on success, -1 on error.
>>>>>>> 3dab50f (Complete PES VCS implementation)
int object_read(const ObjectID *id, ObjectType *type_out, void **data_out, size_t *len_out) {
    if (!id || !type_out || !data_out || !len_out) return -1;

    char path[512];
    object_path(id, path, sizeof(path));

    FILE *f = fopen(path, "rb");
    if (!f) return -1;

    if (fseek(f, 0, SEEK_END) != 0) {
        fclose(f);
        return -1;
    }

    long file_size = ftell(f);
    if (file_size < 0) {
        fclose(f);
        return -1;
    }

    rewind(f);

unsigned char *buffer = malloc((size_t)file_size);
if (!buffer) {
    fclose(f);
    return -1;
}

    if (fread(buffer, 1, (size_t)file_size, f) != (size_t)file_size) {
        free(buffer);
        fclose(f);
        return -1;
    }
    fclose(f);

    ObjectID actual;
    compute_hash(buffer, (size_t)file_size, &actual);
    if (memcmp(actual.hash, id->hash, HASH_SIZE) != 0) {
        free(buffer);
        return -1;
    }

<<<<<<< HEAD
    uint8_t *nul = memchr(buffer, '\0', (size_t)file_size);
=======
    unsigned char *nul = memchr(buffer, '\0', (size_t)file_size);
>>>>>>> 3dab50f (Complete PES VCS implementation)
    if (!nul) {
        free(buffer);
        return -1;
    }

    size_t header_len = (size_t)(nul - buffer);
    char header[64];
    if (header_len >= sizeof(header)) {
        free(buffer);
        return -1;
    }
<<<<<<< HEAD
=======

>>>>>>> 3dab50f (Complete PES VCS implementation)
    memcpy(header, buffer, header_len);
    header[header_len] = '\0';

    char type_str[16];
    size_t payload_len;
    if (sscanf(header, "%15s %zu", type_str, &payload_len) != 2) {
        free(buffer);
        return -1;
    }

<<<<<<< HEAD
    if (strcmp(type_str, "blob") == 0) *type_out = OBJ_BLOB;
    else if (strcmp(type_str, "tree") == 0) *type_out = OBJ_TREE;
    else if (strcmp(type_str, "commit") == 0) *type_out = OBJ_COMMIT;
    else {
=======
    if (strcmp(type_str, "blob") == 0) {
        *type_out = OBJ_BLOB;
    } else if (strcmp(type_str, "tree") == 0) {
        *type_out = OBJ_TREE;
    } else if (strcmp(type_str, "commit") == 0) {
        *type_out = OBJ_COMMIT;
    } else {
>>>>>>> 3dab50f (Complete PES VCS implementation)
        free(buffer);
        return -1;
    }

    size_t actual_payload_len = (size_t)file_size - header_len - 1;
    if (payload_len != actual_payload_len) {
        free(buffer);
        return -1;
    }

<<<<<<< HEAD
    void *payload = malloc(payload_len ? payload_len : 1);
=======
    void *payload = malloc(payload_len > 0 ? payload_len : 1);
>>>>>>> 3dab50f (Complete PES VCS implementation)
    if (!payload) {
        free(buffer);
        return -1;
    }

<<<<<<< HEAD
    memcpy(payload, nul + 1, payload_len);
=======
    if (payload_len > 0) {
        memcpy(payload, nul + 1, payload_len);
    }
>>>>>>> 3dab50f (Complete PES VCS implementation)

    *data_out = payload;
    *len_out = payload_len;

    free(buffer);
    return 0;
}
