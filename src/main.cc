#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <cstddef>
#include <cerrno>
#include <fcntl.h>
#include <unistd.h>
#include <iostream>
#include <string>

#include "util.h"

const uint32_t PAGE_SIZE = 4096;
#define TABLE_MAX_PAGES 100

struct InputBuffer {
  std::string buffer;
};

enum MetaCommandResult {
  META_COMMAND_SUCCESS,
  META_COMMAND_UNRECOGNIZED_COMMAND,
};
enum PrepareResult {
  PREPARE_SUCCESS,
  PREPARE_NEGATIV_ID,
  PREPARE_STRING_TOO_LONG,
  PREPARE_SYNTAX_ERROR,
  PREPARE_UNRECOGNIZED_STATEMENT,
};
enum ExecuteResult {
  EXECUTE_SUCCESS,
  EXECUTE_FAIL,
  EXECUTE_TABLE_FULL,
};

enum StatementType {
  STATEMENT_INSERT,
  STATEMENT_SELECT,
};

#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255

struct Row {
  uint32_t id;
  char username[COLUMN_USERNAME_SIZE + 1];
  char email[COLUMN_EMAIL_SIZE + 1];
};

#define size_of_attribute(Struct, Attribute) \
  sizeof(((Struct*)0)->Attribute)

const uint32_t ID_SIZE = size_of_attribute(Row, id);
const uint32_t USERNAME_SIZE = size_of_attribute(Row, username);
const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

void serialize_row(Row* source, void* destination)
{
  memcpy((char*)destination + ID_OFFSET, &(source->id), ID_SIZE);
  strncpy((char*)destination + USERNAME_OFFSET, source->username, USERNAME_SIZE);
  strncpy((char*)destination + EMAIL_OFFSET, source->email, EMAIL_SIZE);
}

void deserialize_row(void* source, Row* destination)
{
  memcpy(&(destination->id), (const char*)source + ID_OFFSET, ID_SIZE);
  memcpy(&(destination->username), (const char*)source + USERNAME_OFFSET, USERNAME_SIZE);
  memcpy(&(destination->email), (const char*)source + EMAIL_OFFSET, EMAIL_SIZE);
}

void print_row(const Row* row)
{
  printf("(%d, %s, %s)\n", row->id, row->username, row->email);
}

enum NodeType {
  NODE_INTERNAL = 0,
  NODE_LEAF = 1,
};

/// @brief common node header layout
/**
 * struct CommonNode {
 *  uint8_t type;
 *  bool is_root;
 *  Node* parent;
 * };
 * struct InternalNode : CommonNode {
 *  ...
 * };
 */
const uint32_t NODE_TYPE_SIZE = sizeof(uint8_t);
const uint32_t NODE_TYPE_OFFSET = 0;
const uint32_t IS_ROOT_SIZE = sizeof(uint8_t);
const uint32_t IS_ROOT_OFFSET = NODE_TYPE_SIZE;
const uint32_t PARENT_POINTER_SIZE = sizeof(uint64_t);
const uint32_t PARENT_POINTER_OFFSET = IS_ROOT_OFFSET + IS_ROOT_SIZE;
const uint8_t COMMON_NODE_HEADER_SIZE = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;

/// leaf node header layout
/**
 * each node takes a page space
 * struct LeafNode : public CommonNode {
 *  uint32_t num_cells;
 *  struct Entry {
 *    uint32_t key;
 *    Value value;
 *  };
 *  enum {
 *    SPACE = PAGE_SIZE - sizeof(CommonNode) - sizeof(num_cells),
 *    N = SPACE / sizeof(Entry),
 *    P = PAGE_SIZE - PAGE_SIZE,
 *  };
 *  Entry entries[N];
 *  char padding[P];
 * };
 */
const uint32_t LEAF_NODE_NUM_CELLS_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE;
/// @brief  a body of a leaf node is an array of cells
const uint32_t LEAF_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_KEY_OFFSET = 0;
const uint32_t LEAF_NODE_VALUE_SIZE = ROW_SIZE;
const uint32_t LEAF_NODE_VALUE_OFFSET = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
const uint32_t LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
const uint32_t LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_MAX_CELLS = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;

void print_constants()
{
  printf("ROW_SIZE: %d\n", ROW_SIZE);
  printf("COMMON_NODE_HEADER_SIZE: %d\n", COMMON_NODE_HEADER_SIZE);
  printf("LEAF_NODE_HEADER_SIZE: %d\n", LEAF_NODE_HEADER_SIZE);
  printf("LEAF_NODE_CELL_SIZE: %d\n", LEAF_NODE_CELL_SIZE);
  printf("LEAF_NODE_SPACE_FOR_CELLS: %d\n", LEAF_NODE_SPACE_FOR_CELLS);
  printf("LEAF_NODE_MAX_CELLS: %d\n", LEAF_NODE_MAX_CELLS);
}

uint32_t* leaf_node_num_cells(void* node)
{
  return (uint32_t*)((char*)node + LEAF_NODE_NUM_CELLS_OFFSET);
}

void* leaf_node_cell(void* node, uint32_t cell_num)
{
  return (char*)node + LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE;
}

uint32_t* leaf_node_key(void* node, uint32_t cell_num)
{
  return (uint32_t*)leaf_node_cell(node, cell_num);
}

void* leaf_node_value(void* node, uint32_t cell_num)
{
  return (char*)leaf_node_cell(node, cell_num) + LEAF_NODE_KEY_SIZE;
}

void initialize_leaf_node(void* node)
{
  *leaf_node_num_cells(node) = 0;
}

/// @brief pager
/// db文件里按照page number的顺序保存着所有pages
/// pager根据B-TREE的请求来读取或者写入对应page到文件对应的位置
struct Pager {
  int file_descriptor;
  uint32_t file_length;
  uint32_t num_pages;
  void* pages[TABLE_MAX_PAGES];
};

struct Table {
  Pager* pager;
  /// @brief 这里只记录着B-TREE的根节点的page number，就可以了。
  /// 之后就可以ask pager去装载这个page，
  /// 之后根据node记录的page number作为指针不断装载其它子节点page
  uint32_t root_page_num;
};

Pager* pager_open(const char* filename)
{
  int fd = open(filename, O_RDWR | O_CREAT, 0777);
  if (fd == -1) {
    printf("Unable to open file %s\n", filename);
    exit(EXIT_FAILURE);
  }
  off_t file_length = lseek(fd, 0, SEEK_END);
  Pager* pager = (Pager*)malloc(sizeof(Pager));
  pager->file_descriptor = fd;
  pager->file_length = file_length;
  /// db文件保存的所有完整的page记录
  pager->num_pages = file_length / PAGE_SIZE;
  if (file_length % PAGE_SIZE != 0) {
    printf("Db file is not a whole number of pages. Currupt file\n");
    exit(EXIT_FAILURE);
  }

  for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
    pager->pages[i] = NULL;
  }
  return pager;
}

void* get_page(Pager* pager, uint32_t page_num)
{
  if (page_num > TABLE_MAX_PAGES) {
    printf("Tried to fetch page number out of bound: %d > %d\n",
      page_num, TABLE_MAX_PAGES);
    exit(EXIT_FAILURE);
  }

  if (pager->pages[page_num] == nullptr) {
    // cache miss, allocate memory and load from file
    void* page = malloc(PAGE_SIZE);
    uint32_t num_pages = pager->file_length / PAGE_SIZE;

    // we might save a partial page at the end of the file
    if (pager->file_length % PAGE_SIZE) {
      num_pages += 1;
    }
    if (page_num <= num_pages) {
      lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
      ssize_t bytes_read = read(pager->file_descriptor, page, PAGE_SIZE);
      if (bytes_read == -1) {
        printf("Error reading file: %d\n", errno);
        exit(EXIT_FAILURE);
      }
    }
    pager->pages[page_num] = page;
    if (page_num >= pager->num_pages) {
      pager->num_pages = page_num + 1;
    }
  }
  return pager->pages[page_num];
}

void pager_flush(Pager* pager, uint32_t page_num)
{
  if (pager->pages[page_num] == nullptr) {
    printf("Tried to flush null page\n");
    exit(EXIT_FAILURE);
  }

  off_t offset = lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
  if (offset == -1) {
    printf("Error seeking: %d\n", errno);
    exit(EXIT_FAILURE);
  }

  ssize_t bytes_written = write(pager->file_descriptor, pager->pages[page_num], PAGE_SIZE);
  if (bytes_written == -1) {
    printf("Error writing: %d\n", errno);
    exit(EXIT_FAILURE);
  }
}

Table* db_open(const char* filename)
{
  Pager* pager = pager_open(filename);

  Table* table = (Table*)malloc(sizeof(Table));
  table->pager = pager;
  table->root_page_num = 0;  // first page
  if (pager->num_pages == 0) {
    // new data file, initialize page 0 as leaf node
    void* root_node = get_page(pager, 0);
    initialize_leaf_node(root_node);
  }
  return table;
}

void db_close(Table* table)
{
  Pager* pager = table->pager;
  uint32_t num_full_pages = pager->num_pages;
  for (uint32_t i = 0; i < num_full_pages; i++) {
    if (pager->pages[i] == nullptr) {
      continue;
    }
    pager_flush(pager, i);
    free(pager->pages[i]);
    pager->pages[i] = nullptr;
  }

  int result = close(pager->file_descriptor);
  if (result == -1) {
    printf("Error closing db file\n");
    exit(EXIT_FAILURE);
  }
  for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
    void* page = pager->pages[i];
    if (page) {
      free(page);
      pager->pages[i] = nullptr;
    }
  }
  free(pager);
  free(table);
}

/// @brief cursor points to a cell in a B-tree node
/// 用户层面上来cursor指向一个row record数据
/// 对应b-tree树结构来说，就是指向一个node中的cell
/// 因为一个cell对应一个row record数据
struct Cursor {
  Table* table;
  uint32_t page_num;
  uint32_t cell_num;
  /// indicates a position one past the last element
  bool end_of_table;
};

Cursor* table_start(Table* table)
{
  Cursor* cursor = new Cursor();
  cursor->table = table;
  cursor->page_num = table->root_page_num;
  cursor->cell_num = 0;
  void* root_node = get_page(table->pager, table->root_page_num);
  uint32_t num_cells = *leaf_node_num_cells(root_node);
  cursor->end_of_table = (num_cells == 0);

  return cursor;
}

Cursor* table_end(Table* table)
{
  Cursor* cursor = new Cursor();
  cursor->table = table;
  cursor->page_num = table->root_page_num;
  void* root_node = get_page(table->pager, table->root_page_num);
  uint32_t num_cells = *leaf_node_num_cells(root_node);
  cursor->cell_num = num_cells;
  cursor->end_of_table = true;

  return cursor;
}

void* cursor_value(Cursor* cursor)
{
  uint32_t page_num = cursor->page_num;
  void* page = get_page(cursor->table->pager, page_num);
  return leaf_node_value(page, cursor->cell_num);
}

void cursor_advance(Cursor* cursor)
{
  uint32_t page_num = cursor->page_num;
  void* node = get_page(cursor->table->pager, page_num);

  cursor->cell_num += 1;
  if (cursor->cell_num >= *leaf_node_num_cells(node)) {
    cursor->end_of_table = true;
  }
}

/// 在cursor（某一个key/value cell）之前插入新的key/value
/// 这个过程类似于插入排序，从后往前，将cursor后面的cell往后挪一个位置
void leaf_node_insert(Cursor* cursor, uint32_t key, Row* value)
{
  void* node = get_page(cursor->table->pager, cursor->page_num);

  uint32_t num_cells = *leaf_node_num_cells(node);
  if (num_cells >= LEAF_NODE_MAX_CELLS) {
    // node full
    printf("Need to implement splitting a leaf node.\n");
    exit(EXIT_FAILURE);
  }

  if (cursor->cell_num < num_cells) {
    // make room for new cell
    for (uint32_t i = num_cells; i > cursor->cell_num; i--) {
      memcpy(leaf_node_cell(node, i), leaf_node_cell(node, i - 1),
        LEAF_NODE_CELL_SIZE);
    }
  }

  *(leaf_node_num_cells(node)) += 1;
  *(leaf_node_key(node, cursor->cell_num)) = key;
  serialize_row(value, leaf_node_value(node, cursor->cell_num));
}

void print_leaf_node(void* node)
{
  uint32_t num_cells = *leaf_node_num_cells(node);
  printf("leaf (size %d)\n", num_cells);
  for (uint32_t i = 0; i < num_cells; i++) {
    uint32_t key = *leaf_node_key(node, i);
    printf("  - %d : %d\n", i, key);
  }
}

struct Statement {
  StatementType type;
  Row row_to_insert;  // only used by insert statement
};

InputBuffer* new_input_buffer()
{
  InputBuffer* input_buffer = new InputBuffer;
  return input_buffer;
}

void print_prompt() {
  printf("db > ");
}

void read_input(InputBuffer* input_buffer)
{
  if (!std::getline(std::cin, input_buffer->buffer))
  {
      printf("Error reading input\n");
      exit(EXIT_FAILURE);
  }
}

void close_input_buffer(InputBuffer* input_buffer)
{
  delete input_buffer;
}

MetaCommandResult do_meta_command(InputBuffer* input_buffer, Table* table)
{
  if (input_buffer->buffer == ".exit") {
    db_close(table);
    printf("byte...\n");
    exit(EXIT_SUCCESS);
  } else if (input_buffer->buffer == ".constants") {
    printf("Constants:\n");
    print_constants();
    return META_COMMAND_SUCCESS;
  } else if (input_buffer->buffer == ".btree") {
    printf("Tree:\n");
    print_leaf_node(get_page(table->pager, 0));
    return META_COMMAND_SUCCESS;
  } else {
    return META_COMMAND_UNRECOGNIZED_COMMAND;
  }
}

PrepareResult prepare_statement(InputBuffer* input_buffer, Statement* statement)
{
  if (strutil::startsWith(input_buffer->buffer, "insert")) {
    statement->type = STATEMENT_INSERT;
    char* keyword = strtok(&input_buffer->buffer[0], " ");
    char* id_string = strtok(NULL, " ");
    char* username = strtok(NULL, " ");
    char* email = strtok(NULL, " ");
    if (id_string == nullptr || username == nullptr || email == nullptr) {
      return PREPARE_SYNTAX_ERROR;
    }

    int id = atoi(id_string);
    if (id < 0) {
      return PREPARE_NEGATIV_ID;
    }
    if (strlen(username) > COLUMN_USERNAME_SIZE) {
      return PREPARE_STRING_TOO_LONG;
    }
    if (strlen(email) > COLUMN_EMAIL_SIZE) {
      return PREPARE_STRING_TOO_LONG;
    }
    statement->row_to_insert.id = id;
    strcpy(statement->row_to_insert.username, username);
    strcpy(statement->row_to_insert.email, email);
    return PREPARE_SUCCESS;
  }
  if (strutil::startsWith(input_buffer->buffer, "select")) {
    statement->type = STATEMENT_SELECT;
    return PREPARE_SUCCESS;
  }

  return PREPARE_UNRECOGNIZED_STATEMENT;
}

ExecuteResult execute_insert(Statement* statement, Table* table)
{
  void* node = get_page(table->pager, table->root_page_num);
  if (*leaf_node_num_cells(node) >= LEAF_NODE_MAX_CELLS) {
    return EXECUTE_TABLE_FULL;
  }

  Row* row_to_insert = &(statement->row_to_insert);
  Cursor* cursor = table_end(table);
  leaf_node_insert(cursor, row_to_insert->id, row_to_insert);
  delete cursor;

  return EXECUTE_SUCCESS;
}

ExecuteResult execute_select(Statement* statement, Table* table)
{
  Cursor* cursor = table_start(table);

  Row row;
  while (!(cursor->end_of_table)) {
    deserialize_row(cursor_value(cursor), &row);
    print_row(&row);
    cursor_advance(cursor);
  }
  delete cursor;

  return EXECUTE_SUCCESS;
}

ExecuteResult execute_statement(Statement* statement, Table* table)
{
  switch (statement->type) {
    case STATEMENT_INSERT:
    return execute_insert(statement, table);
    case STATEMENT_SELECT:
    return execute_select(statement, table);
  }
  return EXECUTE_FAIL;
}

int main(int argc, char** argv)
{
  if (argc < 2) {
    printf("Must supply a database filename.\n");
    exit(EXIT_FAILURE);
  }

  const char* filename = argv[1];
  Table* table = db_open(filename);

  InputBuffer* input_buffer = new_input_buffer();
  while (true) {
    print_prompt();
    read_input(input_buffer);
    if (input_buffer->buffer.empty()) {
      continue;
    }
    //printf("input: %s\n", input_buffer->buffer.c_str());

    if (input_buffer->buffer[0] == '.') {
      switch (do_meta_command(input_buffer, table)) {
      case META_COMMAND_SUCCESS:
        continue;
      case META_COMMAND_UNRECOGNIZED_COMMAND:
        printf("Unrecognized command '%s'\n", input_buffer->buffer.c_str());
        continue;
      default:
        break;
      }
    }

    Statement statement;
    switch (prepare_statement(input_buffer, &statement)) {
      case PREPARE_SUCCESS:
      break;
      case PREPARE_NEGATIV_ID:
      printf("ID must be positive.\n");
      continue;
      case PREPARE_STRING_TOO_LONG:
      printf("String is too long.\n");
      continue;
      case PREPARE_SYNTAX_ERROR:
      printf("Syntax error. Could not parse statement.\n");
      continue;
      case PREPARE_UNRECOGNIZED_STATEMENT:
      printf("Unrecognized keyword at start of '%s'\n", input_buffer->buffer.c_str());
      continue; // go to wait for input
    }

    switch (execute_statement(&statement, table)) {
      case EXECUTE_SUCCESS:
      printf("Executed.\n");
      break;
      case EXECUTE_TABLE_FULL:
      printf("Error: Table full.\n");
      break;
      default:
      break;
    }
  }
  return 0;
}
