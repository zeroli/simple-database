cmake_minimum_required(VERSION 3.17)

aux_source_directory(. SRC)

add_executable(simple-db ${SRC})
