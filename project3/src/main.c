#include "disk_based_bpt.h"

int main() {
    char tmp[120];
    int i, result;
    char instruction;
    int64_t input;
    open_table("datafile");
    printf("> ");
    while (scanf("%c", &instruction) != EOF) {
        switch (instruction) {
        case 'd':
            scanf("%lld", &input);
            db_delete(input);
            _print_tree();
            break;
        case 'i':
            scanf("%lld", &input);
            scanf("%s", tmp);
            db_insert(input, tmp);
            _print_tree();
            break;
        case 'f':
            scanf("%lld", &input);
            result = db_find(input, tmp);
            printf("result : %d, value : %s\n", result, result ? "" : tmp);
        case 'p':
            _print_keys();
            break;
        case 'q':
            while (getchar() != (int)'\n');
            return EXIT_SUCCESS;
            break;
        case 't':
            _print_tree();
            break;
        case 'l':
            _print_leaves();
            break;
        default:
            break;
        }
        while (getchar() != (int)'\n');
        printf("> ");
    }
    printf("\n");
    close_table();
    return 0;
}