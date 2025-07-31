cmake_minimum_required(VERSION 3.12)
project(rpclcpp VERSION 1.0.0)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Find required packages
find_package(PkgConfig REQUIRED)
pkg_check_modules(HIREDIS REQUIRED hiredis)

# Find nlohmann/json
find_package(nlohmann_json REQUIRED)

# Include directories
include_directories(include)
include_directories(${HIREDIS_INCLUDE_DIRS})

# Source files
set(SOURCES
    src/communication_manager.cpp
    src/knowledge_manager.cpp
    src/logger.cpp
    src/node.cpp
)

# Create library
add_library(${PROJECT_NAME} SHARED ${SOURCES})

# Link libraries
target_link_libraries(${PROJECT_NAME} 
    ${HIREDIS_LIBRARIES}
    nlohmann_json::nlohmann_json
    pthread
)

# Compiler flags
target_compile_options(${PROJECT_NAME} PRIVATE ${HIREDIS_CFLAGS_OTHER})

# Install targets
install(TARGETS ${PROJECT_NAME}
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
)

install(DIRECTORY include/
    DESTINATION include/rpclcpp
    FILES_MATCHING PATTERN "*.hpp"
)

# Examples subdirectory
add_subdirectory(examples)
