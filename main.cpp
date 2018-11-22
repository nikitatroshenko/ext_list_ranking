#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <cstring>
#include <cassert>
#include <cmath>
#include <queue>
#include <algorithm>
#include <functional>

#ifndef DEFAULT_MEMORY_SIZE
#define DEFAULT_MEMORY_SIZE (32768)
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

typedef int(*comparator_func_t)(const void *, const void *);

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
    size_t ram_size_elements;
    run_pool_t *runs;
    bool self_alloc;

    bool write_output_size = true;

    merger_t(void *ram, size_t ram_size_bytes) :
            ram((element_T *) ram),
            ram_size_elements(ram_size_bytes / sizeof(element_T)),
            runs(nullptr),
            self_alloc(false) {}

    void split_into_runs(FILE *in, comparator_func_t cmp) {
        elements_size_t size;

        fread(&size, sizeof size, 1, in);
        size_t runs_cnt = (size != 0) ? 1 + ((size - 1) / ram_size_elements) : 0; // ceiling

        delete runs;
        runs = run_pool_t::of_size(runs_cnt + 1);

        for (size_t i = 0; i < runs_cnt; i++) {
            auto read = static_cast<elements_size_t>(((i + 1) * ram_size_elements <= size) ? ram_size_elements : (size % ram_size_elements));

            fread(ram, sizeof *ram, read, in);
            qsort(ram, read, sizeof *ram, cmp);

            run_t *run = runs->get();
            fwrite(&read, sizeof read, 1, run->file);
            fwrite(ram, sizeof *ram, read, run->file);
            runs->put(run);
        }
    }

    void merge(FILE *files[], size_t rank, FILE *result, comparator_func_t cmp) {
        struct input {
            FILE *file{};
            element_T val{};
            elements_size_t size = 0;
            bool read = false;
        } *inputs = new input[rank]();
        elements_size_t result_size = 0;

        for (size_t i = 0; i < rank; i++) {
            auto &inp = inputs[i];
            inp.file = files[i];
            fread(&inp.size, sizeof inp.size, 1, inp.file);
            result_size += inp.size;
        }
        if (write_output_size) {
            fwrite(&result_size, sizeof result_size, 1, result);
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

    void do_merge_sort(
            FILE *in,
            FILE *out,
            comparator_func_t cmp = cmp_elements<element_T>,
            size_t rank = DEFAULT_MERGE_RANK) {
        split_into_runs(in, cmp);
        size_t block_size = ram_size_elements / 2 / (rank);
        size_t result_block_size = ram_size_elements / 2;
        auto *result_block = ram + rank * block_size;

        run_t *result = runs->get(result_block, sizeof *ram, result_block_size);

        if (runs->size() == 0) {
            // should never happen since N > 1
            fseek(in, 0, SEEK_SET);
            merge(&in, 1, out, cmp);
            return;
        }
        auto files = new FILE *[rank];
        auto **used_runs = new run_t *[rank];

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
        used_runs[0] = runs->get(ram, sizeof *ram, ram_size_elements);

        this->write_output_size = write_output_size;
        merge(&used_runs[0]->file, 1, out, cmp);
        runs->release(used_runs[0]);
        runs->release(result);

        delete[] used_runs;
        delete[] files;
    }

    void sort(
            const char *input_name,
            const char *output_name,
            comparator_func_t cmp = cmp_elements<element_T>,
            size_t merge_rank = DEFAULT_MERGE_RANK) {
        FILE *input = fopen(input_name, "rb");
        FILE *output = fopen(output_name, "wb");

        do_merge_sort(input, output, cmp, merge_rank);

        fclose(input);
        fclose(output);
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


tuple<uint32_t, 3> &join_deuces(
        const tuple<uint32_t, 2> &l,
        const tuple<uint32_t, 2> &r,
        tuple<uint32_t, 3> &res) {
    res.elem[0] = l.elem[0];
    res.elem[1] = l.elem[1];
    res.elem[2] = r.elem[1];
    return res;
}

template<
        typename element_T,
        typename left_src_T,
        typename right_src_T,
        typename target_T
>
struct joiner_t {

    typedef left_src_T left_src_t;
    typedef right_src_T right_src_t;
    typedef target_T target_t;
    typedef std::function<target_T &(const left_src_t &, const right_src_t &, target_t &)> joiner_func_t;

    char *ram;
    size_t ram_size_elements;
    size_t ram_size_bytes;
    bool self_alloc;

    joiner_t(char *ram, size_t ram_size_bytes) :
            ram(ram),
            ram_size_elements(ram_size_bytes / sizeof(element_T)),
            ram_size_bytes(ram_size_bytes),
            self_alloc(false) {}

    void join(
            FILE *left,
            FILE *right,
            FILE *result,
            joiner_func_t joiner_func) {
        elements_size_t left_size;
        elements_size_t right_size;
        fread(&left_size, sizeof left_size, 1, left);
        fread(&right_size, sizeof right_size, 1, right);
        fwrite(&left_size, sizeof left_size, 1, result);

        for (elements_size_t i = 0; i < left_size; i++) {
            left_src_t l{};
            right_src_t r{};
            target_t res{};

            fread(&l, sizeof l, 1, left);
            fread(&r, sizeof r, 1, right);
            res = joiner_func(l, r, res);
            fwrite(&res, sizeof res, 1, result);
        }
    }

    void join(
            const char *left_name,
            const char *right_name,
            const char *result_name,
            joiner_func_t joiner_func) {
        FILE *left = fopen(left_name, "rb");
        FILE *right = fopen(right_name, "rb");
        FILE *result = fopen(result_name, "wb");

        size_t block_size = ram_size_bytes / 4;

        setvbuf(left, ram, _IOFBF, block_size);
        setvbuf(right, ram + block_size, _IOFBF, block_size);
        setvbuf(result, ram + 2 * block_size, _IOFBF, ram_size_bytes - 2 * block_size);

        join(left, right, result, joiner_func);

        fclose(left);
        fclose(right);
        fclose(result);
    }

    ~joiner_t() {
        if (self_alloc) {
            delete[] ram;
        }
    }
};

template<typename src_T, typename target_T>
struct mapper_t {

//    typedef target_T &(*mapper_func_t)(const src_T &, target_T &);
    typedef std::function<target_T &(const src_T &, target_T &)> mapper_func_t;

    char *ram;
    size_t ram_size;
    bool write_output_size = true;

    mapper_t(char *ram, size_t ram_size) :
            ram(ram),
            ram_size(ram_size) {}

    void map(
            const char *src_name,
            const char *target_name,
            mapper_func_t mapper_func) {
        FILE *src = fopen(src_name, "rb");
        FILE *target = fopen(target_name, "wb");

        size_t src_buf_size = ram_size / (sizeof(src_T) + sizeof(target_T)) * sizeof(src_T);
        size_t target_buf_size = ram_size - src_buf_size;

        setvbuf(src, ram, _IOFBF, src_buf_size);
        setvbuf(target, ram + src_buf_size, _IOFBF, target_buf_size);
        map(src, target, mapper_func);

        fclose(src);
        fclose(target);
    }

    void map(FILE *src, FILE *target, mapper_func_t mapper_func) {
        elements_size_t size = 0;
        src_T src_val{};
        target_T target_val{};

        fread(&size, sizeof size, 1, src);
        if (write_output_size) {
            fwrite(&size, sizeof size, 1, target);
        }
        for (elements_size_t i = 0; i < size; i++) {
            fread(&src_val, sizeof src_val, 1, src);
            target_val = mapper_func(src_val, target_val);
            fwrite(&target_val, sizeof target_val, 1, target);
        }
    }
};

template<typename element_T, size_t idx>
static int cmp_by(const void *l, const void *r) {
    auto *lhs = (element_T *) l;
    auto *rhs = (element_T *) r;

    return lhs[idx] - rhs[idx];
}

typedef tuple<uint32_t, 2> pair;
typedef tuple<uint32_t, 3> three;
typedef tuple<uint32_t, 4> four;

int main() {
    const char *input = DEFAULT_INPUT_PATTERN;
    const char *output = DEFAULT_OUTPUT;
    size_t ram_size = DEFAULT_MEMORY_SIZE * 3;
    auto *ram = new char[ram_size];

    typedef joiner_t<uint32_t, pair, pair, three> joiner32_t;
    auto deuce_merger = merger_t<joiner32_t::left_src_t>(
            (joiner32_t::left_src_t *) ram, ram_size);

    deuce_merger.sort(input, BY_FIRST_FILE_NAME, cmp_by<uint32_t, 0>);
    deuce_merger.sort(input, BY_SECOND_FILE_NAME, cmp_by<uint32_t, 1>);

    auto joiner = joiner32_t(ram, ram_size);
    joiner.join(BY_SECOND_FILE_NAME, BY_FIRST_FILE_NAME, RESULT_FILE_NAME, join_deuces);

    auto set_merger = merger_t<joiner32_t::target_t>(
            (joiner32_t::target_t *) ram, ram_size);
    set_merger.write_output_size = false;
    set_merger.sort(RESULT_FILE_NAME, output, cmp_by<uint32_t, 0>);

    delete[] ram;
    return 0;
}