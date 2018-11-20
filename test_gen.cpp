#include <cstdlib>
#include <cstdio>
#include <cstdint>
#include <random>
#include <cstring>
#include <algorithm>

#ifndef DEFAULT_BLOCK_SIZE
#define DEFAULT_BLOCK_SIZE (1 << 20)
#endif

#define DEFAULT_INPUT_PATTERN ("input.bin")
#define DEFAULT_OUTPUT ("output.expected.bin")


template<typename element_T, size_t len>
struct tuple {
    element_T elem[len];
};

template<typename element_T, size_t idx>
static int cmp_by(const void *l, const void *r) {
    auto *lhs = (element_T *) l;
    auto *rhs = (element_T *) r;

    return lhs[idx] - rhs[idx];
}

int main(int argc, char const *argv[])
{
    FILE *input = fopen(DEFAULT_INPUT_PATTERN, "wb");
    FILE *output = fopen(DEFAULT_OUTPUT, "wb");
    uint32_t size;
    std::random_device rd;
    std::mt19937 mt(rd());
    std::uniform_int_distribution<uint64_t> uid;


    if (argc < 2) {
        fprintf(stderr, "Usage: ./test_gen.out <file_size>\n");
        return EXIT_FAILURE;
    }

    size = strtoul(argv[1], nullptr, 10);

    setvbuf(output, nullptr, _IOFBF, DEFAULT_BLOCK_SIZE);
    setvbuf(input, nullptr, _IOFBF, DEFAULT_BLOCK_SIZE);
    fwrite(&size, sizeof size, 1, input);
    auto *list = new uint32_t[size]();
    auto *edges = new uint32_t[2 * size]();

    for (size_t i = 0; i < size; i++) {
        list[i] = uint32_t(i + 1);
    }
    std::shuffle(list, list + size, mt);

    for (size_t i = 0; i < size; i++) {
        edges[2 * i] = list[i];
        edges[2 * i + 1] = list[(i + 1) % size];
    }

    auto *compressed_edges = (uint64_t *) edges;
    std::shuffle(compressed_edges, compressed_edges + size, mt);

    auto min_elem = *list;
    size_t min_elem_idx = 0;
    for (uint64_t i = 1; i < size; i++) {
        if (list[i] < min_elem) {
            min_elem = list[i];
            min_elem_idx = i;
        }
    }

    for (uint64_t i = 0; i < size; i++) {
        fwrite(edges + 2 * i, sizeof *edges, 1, input);
        fwrite(edges + 2 * i + 1, sizeof *edges, 1, input);
        fwrite(list + (min_elem_idx + i) % size, sizeof *list, 1, output);
    }

    fclose(input);
    fclose(output);

    delete[] list;
    delete[] edges;
    return EXIT_SUCCESS;
}
