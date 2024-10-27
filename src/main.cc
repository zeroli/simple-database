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

const uint32_t PAGE_SIZE = 4096;
#define TABLE_MAX_PAGES 100
const uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
const uint32_t TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;

struct Pager {
  int file_descriptor;
  uint32_t file_length;
  void* pages[TABLE_MAX_PAGES];
};

struct Table {
  Pager* pager;
  uint32_t num_rows;
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
  }
  return pager->pages[page_num];
}

void pager_flush(Pager* pager, uint32_t page_num, uint32_t size)
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

  ssize_t bytes_written = write(pager->file_descriptor, pager->pages[page_num], size);
  if (bytes_written == -1) {
    printf("Error writing: %d\n", errno);
    exit(EXIT_FAILURE);
  }
}

Table* db_open(const char* filename)
{
  Pager* pager = pager_open(filename);
  uint32_t num_rows = pager->file_length / ROW_SIZE;

  Table* table = (Table*)malloc(sizeof(Table));
  table->num_rows = num_rows;
  table->pager = pager;
  return table;
}

void db_close(Table* table)
{
  Pager* pager = table->pager;
  uint32_t num_full_pages = table->num_rows / ROWS_PER_PAGE;
  for (uint32_t i = 0; i < num_full_pages; i++) {
    if (pager->pages[i] == nullptr) {
      continue;
    }
    pager_flush(pager, i, PAGE_SIZE);
    free(pager->pages[i]);
    pager->pages[i] = nullptr;
  }
  /// there might be a partial page to write to the end of the file
  uint32_t num_additional_rows = table->num_rows % ROWS_PER_PAGE;
  if (num_additional_rows > 0) {
    uint32_t page_num = num_full_pages;
    if (pager->pages[page_num] != nullptr) {
      pager_flush(pager, page_num, num_additional_rows * ROW_SIZE);
      free(pager->pages[page_num]);
      pager->pages[page_num] = nullptr;
    }
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

/// @brief cursor points to a row of table
struct Cursor {
  Table* table;
  uint32_t row_num;
  /// indicates a position one past the last element
  bool end_of_table;
};

Cursor* table_start(Table* table)
{
  Cursor* cursor = new Cursor();
  cursor->table = table;
  cursor->row_num = 0;
  cursor->end_of_table = (table->num_rows == 0);

  return cursor;
}

Cursor* table_end(Table* table)
{
  Cursor* cursor = new Cursor();
  cursor->table = table;
  cursor->row_num = table->num_rows;
  cursor->end_of_table = true;

  return cursor;
}

void* cursor_value(Cursor* cursor)
{
  uint32_t row_num = cursor->row_num;
  uint32_t page_num = row_num / ROWS_PER_PAGE;
  void* page = get_page(cursor->table->pager, page_num);
  uint32_t row_offset = row_num % ROWS_PER_PAGE;
  uint32_t byte_offset = row_offset * ROW_SIZE;
  return (char*)page + byte_offset;
}

void cursor_advance(Cursor* cursor)
{
  cursor->row_num += 1;
  if (cursor->row_num >= cursor->table->num_rows) {
    cursor->end_of_table = true;
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
  if (table->num_rows >= TABLE_MAX_ROWS) {
    return EXECUTE_TABLE_FULL;
  }

  Row* row_to_insert = &(statement->row_to_insert);
  Cursor* cursor = table_end(table);
  /// insert, just append
  serialize_row(row_to_insert, cursor_value(cursor));
  table->num_rows += 1;
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
