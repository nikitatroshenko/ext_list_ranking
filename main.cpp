#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <cstring>
#include <cassert>
#include <cmath>
#include <queue>
#include <algorithm>
#include <functional>
#include <stack>
#include <random>

#ifndef DEFAULT_MEMORY_SIZE
#define DEFAULT_MEMORY_SIZE (204800)
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
#define SEVEN_NAME_PATTERN "/tmp/seven.%d.bin"
#define FIVE_NAME_PATTERN "/tmp/five.%d.bin"
#define RANKED_NAME_PATTERN "/tmp/ranked.%d.bin"
#define WEIGHTED_NAME_PATTERN "/tmp/weighted.%d.bin"

#define JOIN_LEFT_NAME "/tmp/join.left.tmp.bin"
#define JOIN_RIGHT_NAME "/tmp/join.right.tmp.bin"
#define JOIN_RESULT_NAME "/tmp/join.result.tmp.bin"
#else
#define RUN_NAME_PATTERN "run.%d.bin"
#define SEVEN_NAME_PATTERN "seven.%d.bin"
#define FIVE_NAME_PATTERN "five.%d.bin"
#define RANKED_NAME_PATTERN "ranked.%d.bin"
#define WEIGHTED_NAME_PATTERN "weighted.%d.bin"

#define JOIN_LEFT_NAME "join.left.tmp.bin"
#define JOIN_RIGHT_NAME "join.right.tmp.bin"
#define JOIN_RESULT_NAME "join.result.tmp.bin"
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

        assert(block_size > 0);
        assert(result_block_size > 0);
        assert((rank * block_size + result_block_size) <= ram_size_elements);

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

    typedef element_T element_t;
};

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
        typename left_src_T,
        typename right_src_T,
        typename target_T
>
struct joiner_t {

    typedef left_src_T left_src_t;
    typedef right_src_T right_src_t;
    typedef target_T target_t;
    typedef std::function<bool(const left_src_t &, const right_src_t &, target_t &)> joiner_func_t;
    typedef std::function<bool(const left_src_t &, const right_src_t &)> joiner_predicate_t;

    char *ram;
    size_t ram_size_bytes;
    bool self_alloc;

    joiner_t(char *ram, size_t ram_size_bytes) :
            ram(ram),
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
            joiner_func(l, r, res);
            fwrite(&res, sizeof res, 1, result);
        }
    }

    void left_join(
            FILE *left,
            FILE *right,
            FILE *result,
            joiner_func_t joiner_func
    ) {
        elements_size_t left_size;
        elements_size_t right_size;
        fread(&left_size, sizeof left_size, 1, left);
        fread(&right_size, sizeof right_size, 1, right);
        fwrite(&left_size, sizeof left_size, 1, result);
        bool right_consumed = true;

        left_src_t  l{};
        right_src_t r{};
        target_t res{};
        for (elements_size_t i = 0; i < left_size; i++) {
            if (right_consumed) {
                fread(&r, sizeof r, 1, right);
            }
            fread(&l, sizeof l, 1, left);
            right_consumed = joiner_func(l, r, res);
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

        size_t left_block_size = ram_size_bytes * sizeof(left_src_t)
                / (sizeof(left_src_t) + sizeof(right_src_t) + sizeof(target_t));
        size_t right_block_size = ram_size_bytes * sizeof(right_src_t)
                / (sizeof(left_src_t) + sizeof(right_src_t) + sizeof(target_t));
        size_t result_block_size = ram_size_bytes - left_block_size - right_block_size;

        assert(left_block_size > 0);
        assert(right_block_size > 0);
        assert(result_block_size > 0);
        assert((left_block_size + right_block_size + result_block_size) <= ram_size_bytes);

        setvbuf(left, ram, _IOFBF, left_block_size);
        setvbuf(right, ram + left_block_size, _IOFBF, right_block_size);
        setvbuf(result, ram + left_block_size + right_block_size, _IOFBF, result_block_size);

        left_join(left, right, result, joiner_func);

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

    typedef std::function<bool(const src_T &, target_T &)> mapper_func_t;
    typedef std::function<bool(const src_T &)> filter_predicate_t;

    char *ram;
    size_t ram_size;
    bool write_output_size = true;

    mapper_t(char *ram, size_t ram_size) :
            ram(ram),
            ram_size(ram_size) {}

    static bool identity(const src_T &src, target_T &target) {
        target = src;
        return true;
    }

    elements_size_t map(
            const char *src_name,
            const char *target_name,
            mapper_func_t mapper_func = identity) {
        FILE *src = fopen(src_name, "rb");
        FILE *target = fopen(target_name, "wb");

        size_t src_buf_size = ram_size / (sizeof(src_T) + sizeof(target_T)) * sizeof(src_T);
        size_t target_buf_size = ram_size - src_buf_size;

        assert(src_buf_size > 0);
        assert(target_buf_size > 0);
        assert((src_buf_size + target_buf_size) <= ram_size);

        setvbuf(src, ram, _IOFBF, src_buf_size);
        setvbuf(target, ram + src_buf_size, _IOFBF, target_buf_size);
        auto cnt = map(src, target, mapper_func);

        fclose(src);
        fclose(target);
        return cnt;
    }

    elements_size_t map(
            FILE *src,
            FILE *target,
            mapper_func_t mapper_func) {
        elements_size_t size = 0;
        elements_size_t result_size = 0;
        src_T src_val{};
        target_T target_val{};

        fread(&size, sizeof size, 1, src);
        if (write_output_size) {
            fwrite(&size, sizeof size, 1, target);
        }
        for (elements_size_t i = 0; i < size; i++) {
            fread(&src_val, sizeof src_val, 1, src);
            if(!mapper_func(src_val, target_val)) {
                continue;
            }
            fwrite(&target_val, sizeof target_val, 1, target);
            result_size++;
        }
        if (write_output_size) {
            fseek(target, 0, SEEK_SET);
            fwrite(&result_size, sizeof result_size, 1, target);
        }

        return result_size;
    }
};

template<typename element_T, size_t idx>
static int cmp_by(const void *l, const void *r) {
    auto *lhs = (element_T *) l;
    auto *rhs = (element_T *) r;

    if (lhs[idx] < rhs[idx]) {
        return -1;
    } else if (lhs[idx] > rhs[idx]) {
        return 1;
    } else {
        return 0;
    }
}

typedef tuple<uint32_t, 2> pair;
typedef tuple<uint32_t, 3> three;
typedef tuple<uint32_t, 4> four;
typedef tuple<uint32_t, 6> six;
typedef tuple<uint32_t, 7> seven;
typedef tuple<uint32_t, 8> eight;
typedef tuple<uint32_t, 9> nine;

static const char *const format_name(const char *pattern, uint32_t id) {
    static char buf[MAX_PATH];

    sprintf(buf, pattern, id);
    return buf;
}

int main() {
    const char *input = DEFAULT_INPUT_PATTERN;
    const char *output = DEFAULT_OUTPUT;
    size_t ram_size = DEFAULT_MEMORY_SIZE;
    auto *ram = new char[ram_size];

    auto weight_appender = mapper_t<pair, three>(ram, ram_size);

    uint32_t iteration = 0;
    weight_appender.map(
            input,
            format_name(WEIGHTED_NAME_PATTERN, iteration),
            [](const pair &src, three &target) {
                target.elem[0] = src.elem[0];   // i
                target.elem[1] = src.elem[1];   // n(i)
                target.elem[2] = 1;             // w(i)
                return true;
            });

    std::random_device rd{};
    std::mt19937 mt(rd());
//    std::mt19937 mt(0);
    std::uniform_int_distribution<uint32_t> uid(0, 1);

    auto flagger = mapper_t<three, four>(ram, ram_size);
    auto weighted_sorter = merger_t<four>(ram, ram_size);
    auto joined_flagged_sorter = merger_t<six>(ram, ram_size);
    auto flagged_joiner = joiner_t<four, four, six>(ram, ram_size);
    auto mega_seven_joiner = joiner_t<six, six, seven>(ram, ram_size);
    auto list_reducer = mapper_t<seven, three>(ram, ram_size);

    char weighted_name[MAX_PATH]{};
    char seven_name[MAX_PATH]{};

    // reduce source list, output: weighted(iteration), seven(iteration - 1)
    while (true) {
        flagger.map(format_name(WEIGHTED_NAME_PATTERN, iteration),
                    JOIN_RESULT_NAME,
                    [&uid, &mt](const three &src, four &target) {
                        target.elem[0] = src.elem[0];                       // i
                        target.elem[1] = src.elem[1];                       // n(i)
                        target.elem[2] = src.elem[2];                       // w(i)
                        target.elem[3] = static_cast<uint32_t>(uid(mt));    // f(i)
                        return true;
                    });
        weighted_sorter.sort(JOIN_RESULT_NAME, JOIN_LEFT_NAME, cmp_by<uint32_t, 0>);
        weighted_sorter.sort(JOIN_RESULT_NAME, JOIN_RIGHT_NAME, cmp_by<uint32_t, 1>);
        flagged_joiner.join(JOIN_LEFT_NAME, JOIN_RIGHT_NAME, JOIN_RESULT_NAME,
                            [](const four &left, const four &right, six &result) {
                                result.elem[0] = right.elem[0]; // == i
                                result.elem[1] = right.elem[1]; // == n(i) == left.elem[0]
                                result.elem[2] = left.elem[1]; // == n(n(i)) <- is not used
                                result.elem[3] = right.elem[2]; // == w(i)
                                result.elem[4] = right.elem[3]; // == f(i)
                                result.elem[5] = left.elem[3]; // == f(n(i))
                                return true;
                            }); // sorted by result.elem[1]

        joined_flagged_sorter.sort(JOIN_RESULT_NAME, JOIN_LEFT_NAME, cmp_by<uint32_t, 0>);

        strcpy(seven_name, format_name(SEVEN_NAME_PATTERN, iteration));
        mega_seven_joiner.join(
                JOIN_LEFT_NAME,
                JOIN_RESULT_NAME,
                seven_name,
                [](const six &left, const six &right, seven &result) {
                    result.elem[0] = right.elem[0]; // p(j)
                    result.elem[1] = static_cast<uint32_t>(right.elem[4] && !right.elem[5]); // d(p(j)) = f(p(j)) && f(j)
                    result.elem[2] = right.elem[3]; // w(p(j))
                    result.elem[3] = right.elem[1]; //j = i
                    result.elem[4] = left.elem[1]; // n(i)
                    result.elem[5] = static_cast<uint32_t>(left.elem[4] && !left.elem[5]); // d(i) = f(i) && f(n(i))
                    result.elem[6] = left.elem[3]; // w(i)
                    return true;
                }
        ); // sorted by result.elem[3]

        strcpy(weighted_name, format_name(WEIGHTED_NAME_PATTERN, iteration + 1));
        elements_size_t current_size = list_reducer.map(
                seven_name,
                weighted_name,
                [](const seven &src, three &target) {
                    if (!src.elem[1] && !src.elem[5]) { // !d(p(j)) && !d(j)
                        target.elem[0] = src.elem[0]; // p(j)
                        target.elem[1] = src.elem[3]; // j
                        target.elem[2] = src.elem[2]; // w(p(j))
                        return true;
                    } else if (src.elem[5]) { // d(j)
                        target.elem[0] = src.elem[0]; // p(j)
                        target.elem[1] = src.elem[4]; // n(j)
                        target.elem[2] = src.elem[2] + src.elem[6]; // w(p(j)) + w(j)
                        return true;
                    }
                    return false;
                }
        ); // unordered since source was sorted by j and j may be replaced with n(j) sometimes, which is not ordered

        iteration++;
        if (current_size < (ram_size / sizeof(six))) {
            break;
        }
    }

    // solve task in RAM
    {
        auto *weighted = (three *) ram; // i, n(i), w(i)
        auto *ranked = (pair *) (ram + ram_size * 3 / 5); // i, r(i)
        elements_size_t size = 0;

        FILE *weighted_file = fopen(format_name(WEIGHTED_NAME_PATTERN, iteration), "rb");
        FILE *ranked_file = fopen(format_name(RANKED_NAME_PATTERN, iteration), "wb");

        fread(&size, sizeof size, 1, weighted_file);
        fwrite(&size, sizeof size, 1, ranked_file);
        fread(weighted, sizeof *weighted, size, weighted_file);
        qsort(weighted, size, sizeof *weighted, cmp_by<three::element_t, 0>);

        three &cur = weighted[0];

        ranked[0].elem[0] = weighted[0].elem[0];
        ranked[0].elem[1] = 0;
        for (elements_size_t i = 1; i < size; i++) {
            ranked[i].elem[0] = cur.elem[1]; // n(i)
            ranked[i].elem[1] = cur.elem[2] + ranked[i - 1].elem[1]; // w(i) + r(p(i))

            auto next = three{cur.elem[1], 0, 0};
            cur = *(three *) bsearch(&next, weighted, size, sizeof *weighted, cmp_by<three::element_t, 0>);
        }
        // sort by i for further join
        qsort(ranked, size, sizeof *ranked, cmp_by<pair::element_t, 0>);
        fwrite(ranked, sizeof *ranked, size, ranked_file);

        fclose(weighted_file);
        fclose(ranked_file);
    }

    auto curr_rank_joiner = joiner_t<seven, pair, eight>(ram, ram_size);
    auto prev_rank_joiner = joiner_t<eight, pair, nine>(ram, ram_size);
    auto eights_sorter = merger_t<eight>(ram, ram_size);
    auto ranker = mapper_t<nine, pair>(ram, ram_size);

    // restore ranked(i) from ranked(i + 1) and seven(i)
    while (iteration != 0) {
        iteration--;

        // assume that ranked are always sorted by i in previous iteration
        // sevens are already sorted by j
        // <p(j), d(p(j)), w(p(j)), j, n(j), d(j), w(j)> LEFT JOIN <i, r(i)>
        // INTO <p(j), d(p(j)), w(p(j)), j, n(j), d(j), w(j), r(j)>
        strcpy(seven_name, format_name(SEVEN_NAME_PATTERN, iteration));

        curr_rank_joiner.join(
                seven_name,
                format_name(RANKED_NAME_PATTERN, iteration + 1),
                JOIN_RESULT_NAME,
                [](const seven &left, const pair &right, eight &result) {
                    for (size_t i = 0; i < 7; i++) {
                        result.elem[i] = left.elem[i];
                    }
                    if (left.elem[3] == right.elem[0]) { // j == i
                        result.elem[7] = right.elem[1]; // r(j) <- r(i)
                        return true;
                    }
                    return false;
                }
        );
        eights_sorter.sort(JOIN_RESULT_NAME, JOIN_LEFT_NAME, cmp_by<eight::element_t, 0>); // by p(j)
        // <p(j), d(p(j)), w(p(j)), j, n(j), d(j), w(j), r(j)> LEFT JOIN <i, r(i)>
        // INTO <r(p(j), p(j), d(p(j)), w(p(j)), j, n(j), d(j), w(j), r(j)>
        prev_rank_joiner.join(
                JOIN_LEFT_NAME,
                format_name(RANKED_NAME_PATTERN, iteration + 1),
                JOIN_RESULT_NAME,
                [](const eight &left, const pair &right, nine &result) {
                    for (size_t i = 0; i < 8; i++) {
                        result.elem[i + 1] = left.elem[i];
                    }
                    if (left.elem[0] == right.elem[0]) { // p(j) == i
                        result.elem[0] = right.elem[1]; // r(p(j)) <- r(i)
                        return true;
                    }
                    return false;
                }
        ); // sorted by p(j)

        // MAP <r(p(j)), p(j), d(p(j)), w(p(j)), j, n(j), d(j), w(j), r(j)> TO <i, r(i)>
        ranker.map(
                JOIN_RESULT_NAME,
                format_name(RANKED_NAME_PATTERN, iteration),
                [](const nine &src, pair &target) {
                    target.elem[0] = src.elem[1]; // i <- p(j)
                    if (!src.elem[2]) { // !d(p(j))
                        target.elem[1] = src.elem[0]; // r(i) <- r(p(j))
                    } else {
                        target.elem[1] = src.elem[8] - src.elem[3]; // r(i) <- r(j) - w(p(j))
                    }
                    return true;
                }
        ); // sorted by i
    }

    uint32_t min_element_rank;
    {
        FILE *ranked = fopen(format_name(RANKED_NAME_PATTERN, iteration), "rb");
        fseek(ranked, 2 * sizeof(uint32_t), SEEK_SET); // todo: zero-length case
        fread(&min_element_rank, sizeof min_element_rank, 1, ranked);
        fclose(ranked);
    }

    mapper_t<pair, pair>(ram, ram_size).map(
            format_name(RANKED_NAME_PATTERN, iteration),
            JOIN_LEFT_NAME,
            [min_element_rank](const pair &src, pair &target) {
                target = src;
                target.elem[1] -= min_element_rank;
                return true;
            }
    ); // normalize element ranks so that minimal element had rank = 0
    auto ranked_sorter = merger_t<pair>(ram, ram_size);
    ranked_sorter.sort(
            JOIN_LEFT_NAME,
            JOIN_RESULT_NAME,
            cmp_by<pair::element_t, 1>
    ); // by r(i)

    auto rank_remover = mapper_t<pair, uint32_t>(ram, ram_size);
    rank_remover.write_output_size = false;
    rank_remover.map(JOIN_RESULT_NAME, output, [](const pair &src, uint32_t &target) {
        target = src.elem[0];
        return true;
    });

    delete[] ram;
    return 0;
}