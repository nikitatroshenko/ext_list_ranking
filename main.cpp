#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <cstring>
#include <cassert>
#include <cmath>
#include <queue>
#include <algorithm>

#ifndef DEFAULT_MEMORY_SIZE
#define DEFAULT_MEMORY_SIZE (8192)
#endif

#ifndef DEFAULT_MERGE_RANK
#define DEFAULT_MERGE_RANK 8
#endif

#ifndef DEFAULT_BLOCKS_NUMBER
#define DEFAULT_BLOCKS_NUMBER (DEFAULT_MEMORY_SIZE / DEFAULT_BLOCK_SIZE)
#endif

#ifndef DEFAULT_MERGE_RANK
#define DEFAULT_MERGE_RANK 2
#endif

#define DEFAULT_INPUT_PATTERN ("input.bin")
#define DEFAULT_OUTPUT ("output.bin")

#if _LOCAL_TEST
#define RUN_NAME_PATTERN "/tmp/run.%d.bin"
#else
#define RUN_NAME_PATTERN "run.%d.bin"
#endif

#ifndef MAX_PATH
#define MAX_PATH 20
#endif

#ifndef __compar_fn_t
typedef int(*__compar_fn_t)(const void *, const void *);
#endif

struct run_t {
    FILE *file;
    int id;

    explicit run_t(int id) : file(nullptr), id(id) {}

    const char *get_name() {
        static char name[MAX_PATH]{};

        sprintf(name, RUN_NAME_PATTERN, id);
        return name;
    }
};

typedef uint32_t elements_size_t;

struct run_pool_t {
    static int id_counter;

    std::queue<run_t *> runs;

    run_pool_t() : runs() {}

    static run_pool_t *of_size(size_t size) {
        int id_counter = 0;
        auto *pool = new run_pool_t();
        const elements_size_t zero = 0;

        for (size_t i = 0; i < size; i++) {
            auto run = new run_t(id_counter++);
            const char *name = run->get_name();

            run->file = fopen(name, "wb+");
            setvbuf(run->file, nullptr, _IONBF, 0);
            fwrite(&zero, sizeof zero, 1, run->file);
            pool->put(run);
        }
        return pool;
    }

    run_t *get() {
        return get(nullptr, 0, 0);
    }

    run_t *get(void *buf, size_t element_size, size_t elements_cnt) {
        auto run = runs.front();
        run->file = fopen(run->get_name(), "rb+");
        size_t buf_size = element_size * elements_cnt;
        setvbuf(run->file, (char *) buf, buf_size ? _IOFBF : _IONBF, buf_size);
        runs.pop();
        return run;
    }

    void put(run_t *run) {
        fclose(run->file);
        runs.push(run);
    }

    void release(run_t *run) {
        fclose(run->file);
        delete run;
    }

    size_t size() const {
        return runs.size();
    }

    ~run_pool_t() {
        while (!runs.empty()) {
            auto *run = runs.front();
            runs.pop();
            delete run;
        }
    }
};

int run_pool_t::id_counter = 0;

template<typename element_T>
int cmp_elements(const void *l, const void *r) {
    element_T left = *(element_T *) l;
    element_T right = *(element_T *) r;

    if (left < right) {
        return -1;
    } else if (left > right) {
        return 1;
    } else {
        return 0;
    }
}

template<typename element_T>
struct merger_t {
    element_T *ram;
    size_t ram_size;
    run_pool_t *runs;
    bool self_alloc;

    bool write_output_size = true;

    explicit merger_t(size_t ram_size) :
            ram_size(ram_size),
            runs(nullptr),
            self_alloc(true) {
        ram = new element_T[ram_size];
    }

    merger_t(element_T *ram, size_t ram_size) :
            ram(ram),
            ram_size(ram_size),
            runs(nullptr),
            self_alloc(false) {}

    void split_into_runs(FILE *in, __compar_fn_t cmp) {
        elements_size_t size;

        fread(&size, sizeof size, 1, in);
        size_t runs_cnt = (size != 0) ? 1 + ((size - 1) / ram_size) : 0; // ceiling

        delete runs;
        runs = run_pool_t::of_size(runs_cnt + 1);

        for (size_t i = 0; i < runs_cnt; i++) {
            elements_size_t read = ((i + 1) * ram_size <= size) ? ram_size : (size % ram_size);

            fread(ram, sizeof *ram, read, in);
            qsort(ram, read, sizeof *ram, cmp);

            run_t *run = runs->get();
            fwrite(&read, sizeof read, 1, run->file);
            fwrite(ram, sizeof *ram, read, run->file);
            runs->put(run);
        }
    }

    void merge(FILE *files[], size_t rank, FILE *result, __compar_fn_t cmp) {
        struct input { FILE *file{}; element_T val{}; elements_size_t size = 0; bool read = false;} *inputs = new input[rank]();
        elements_size_t ressiz = 0;

        for (size_t i = 0; i < rank; i++) {
            auto &inp = inputs[i];
            inp.file = files[i];
            //inp = {files[i], 0, 0, false};
            fread(&inp.size, sizeof inp.size, 1, inp.file);
            ressiz += inp.size;
        }
        if (write_output_size) {
            fwrite(&ressiz, sizeof ressiz, 1, result);
        }

        while (true) {
            input *min_elem = nullptr;
            for (size_t i = 0; i < rank; i++) {
                auto &inp = inputs[i];
                if (!inp.read && !inp.size) {
                    continue;
                }
                if (!inp.read) {
                    fread(&inp.val, sizeof inp.val, 1, inp.file);
                    inp.read = true;
                    inp.size--;
                }
                if ((min_elem == nullptr) || (cmp(&inp.val, &min_elem->val) < 0)) {
                    min_elem = &inp;
                }
            }
            if (min_elem == nullptr) {
                break;
            }
            fwrite(&min_elem->val, sizeof min_elem->val, 1, result);
            min_elem->read = false;
        }
        delete[] inputs;
    }

    void do_merge_sort(FILE *in, FILE *out, __compar_fn_t cmp = cmp_elements<element_T>, size_t rank = DEFAULT_MERGE_RANK) {
        split_into_runs(in, cmp);
        size_t block_size = ram_size / 2 / (rank);
        size_t result_block_size = ram_size / 2;
        auto *result_block = ram + rank * block_size;

        run_t *result = runs->get(result_block, sizeof *ram, result_block_size);

        if (runs->size() == 0) {
            // should never happen since N > 1
            fseek(in, 0, SEEK_SET);
            merge(&in, 1, out, cmp);
            return;
        }
        auto files = new FILE *[rank];
        auto **used_runs = new run_t*[rank];

        auto write_output_size = this->write_output_size;
        this->write_output_size = true;

        while (runs->size() > 1) {
            size_t files_cnt = 0;
            element_T *block_start = ram;

            for (; (files_cnt < rank) && (runs->size() != 0); files_cnt++, block_start += block_size) {
                used_runs[files_cnt] = runs->get(block_start, sizeof *block_start, block_size);
                files[files_cnt] = used_runs[files_cnt]->file;
            }

            merge(files, files_cnt, result->file, cmp);
            runs->put(result);
            for (size_t i = 1; i < files_cnt; i++) {
                runs->release(used_runs[i]);
            }
            result = used_runs[0];
            freopen(result->get_name(), "rb+", result->file);
            setvbuf(result->file, (char *) block_start, _IOFBF, result_block_size * sizeof *block_start);
        }
        used_runs[0] = runs->get(ram, sizeof *ram, ram_size);

        this->write_output_size = write_output_size;
        merge(&used_runs[0]->file, 1, out, cmp);
        runs->release(used_runs[0]);
        runs->release(result);

        delete[] used_runs;
        delete[] files;
    }

    ~merger_t() {
        if (self_alloc) {
            delete[] ram;
        }
        delete runs;
    }
};

template<typename element_T, size_t len>
struct tuple {
    element_T elem[len];
};

#define BY_FIRST_FILE_NAME ("by_first.tmp.bin")
#define BY_SECOND_FILE_NAME ("by_second.tmp.bin")
#define RESULT_FILE_NAME ("result.tmp.bin")

template<typename element_T>
struct joiner_t {
    element_T *ram;
    size_t ram_size;

    explicit joiner_t(size_t ram_size) :
            ram_size(ram_size) {
        ram = new element_T[3 * ram_size];
    }

    void join(FILE *left, FILE *right, FILE *result) {
        elements_size_t left_size;
        elements_size_t right_size;
        fread(&left_size, sizeof left_size, 1, left);
        fread(&right_size, sizeof right_size, 1, right);
        fwrite(&left_size, sizeof left_size, 1, result);

        for (elements_size_t i = 0; i < left_size; i++) {
            tuple<element_T, 2> l{};
            tuple<element_T, 2> r{};
            tuple<element_T, 3> res{};

            fread(&l, sizeof l, 1, left);
            fread(&r, sizeof r, 1, right);
            res.elem[0] = l.elem[0];
            res.elem[1] = l.elem[1];
            res.elem[2] = r.elem[1];
            fwrite(&res, sizeof res, 1, result);
        }
    }

    template<size_t idx>
    static int cmp_by(const void *l, const void *r) {
        auto *lhs = (element_T *) l;
        auto *rhs = (element_T *) r;

        return lhs[idx] - rhs[idx];
    }

    void do_join(FILE *in, FILE *out) {
        auto *deuce_merger = new merger_t<tuple<element_T, 2>>((tuple<element_T, 2> *) ram, ram_size);
        FILE *by_first = fopen(BY_FIRST_FILE_NAME, "wb+");
        FILE *by_second = fopen(BY_SECOND_FILE_NAME, "wb+");
        FILE *result = fopen(RESULT_FILE_NAME, "wb");

        deuce_merger->do_merge_sort(in, by_first, cmp_by<0>);
        fseek(in, 0, SEEK_SET);
        deuce_merger->do_merge_sort(in, by_second, cmp_by<1>);
        delete deuce_merger;

        size_t block_size = ram_size / 4;

        freopen(BY_FIRST_FILE_NAME, "rb+", by_first);
        setvbuf(by_first, (char *) ram, _IOFBF, block_size * sizeof *ram);
        freopen(BY_SECOND_FILE_NAME, "rb+", by_second);
        setvbuf(by_second, (char *) (ram + block_size), _IOFBF, block_size * sizeof *ram);
        freopen(RESULT_FILE_NAME, "wb+", result);
        setvbuf(result, (char *) (ram + 2 * block_size), _IOFBF, (ram_size - 2 * block_size) * sizeof *ram);

        join(by_second, by_first, result);

        auto *set_merger = new merger_t<tuple<element_T, 3>>((tuple<element_T, 3> *) ram, ram_size);
        set_merger->write_output_size = false;
        fclose(by_first);
        fclose(by_second);
        freopen(RESULT_FILE_NAME, "rb+", result);
        set_merger->do_merge_sort(result, out, cmp_by<0>);
        fclose(result);
        delete set_merger;
    }

    ~joiner_t() {
        delete[] ram;
    }
};

int main() {
    FILE *in = fopen(DEFAULT_INPUT_PATTERN, "rb");
    FILE *out = fopen(DEFAULT_OUTPUT, "wb");

    auto joiner = joiner_t<uint32_t>(DEFAULT_MEMORY_SIZE);
    joiner.do_join(in, out);

    fclose(in);
    fclose(out);
    return 0;
}