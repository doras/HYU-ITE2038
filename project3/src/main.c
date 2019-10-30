#include "disk_based_bpt.h"
#include "print_bpt.h"

int main() {
    char tmp[120];
    int i, result;
    char instruction;
    int table_id;
    int64_t input;
    printf("> ");
    while (scanf("%c", &instruction) != EOF) {
        switch (instruction) {
        case 'd':
            scanf("%lld", &input);
            db_delete(table_id, input);
            // _print_tree(table_id);
            break;
        case 'i':
            scanf("%lld", &input);
            scanf("%s", tmp);
            db_insert(table_id, input, tmp);
            // _print_tree(table_id);
            break;
        case 'f':
            scanf("%lld", &input);
            result = db_find(table_id, input, tmp);
            printf("result : %d, value : %s\n", result, result ? "" : tmp);
        case 'p':
            // _print_table_id();
            // _print_keys(table_id);
            break;
        case 'q':
            while (getchar() != (int)'\n');
            return EXIT_SUCCESS;
            break;
        case 't':
            // _print_tree(table_id);
            break;
        case 'l':
            // _print_leaves(table_id);
            break;
        case 'o':
            scanf("%120s", tmp);
            table_id = open_table(tmp);
            break;
        case 'c':
            close_table(table_id);
            break;
        case 's':
            shutdown_db();
            break;
        case 'n':
            scanf("%d", &result);
            init_db(result);
            break;
        default:
            break;
        }
        while (getchar() != (int)'\n');
        printf("> ");
    }
    printf("\n");
    return 0;
}