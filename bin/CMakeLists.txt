add_executable(labwork_12_mvegorov main.cpp)

target_link_libraries(labwork_12_mvegorov CoolDB)
target_include_directories(labwork_12_mvegorov PRIVATE ${PROJECT_SOURCE_DIR})
