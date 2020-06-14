cmake_minimum_required (VERSION 2.6)

execute_process(COMMAND ${CMAKE_COMMAND}   -E create_symlink ../091_datasetsRucio/output/ ${CMAKE_CURRENT_BINARY_DIR}/input)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/input DESTINATION /opt/dkb/093_datasetsFormat)
install(DIRECTORY ./  DESTINATION /opt/dkb/093_datasetsFormat REGEX "input" EXCLUDE)

