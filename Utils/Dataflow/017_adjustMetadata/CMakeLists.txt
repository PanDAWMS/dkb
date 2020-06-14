cmake_minimum_required (VERSION 2.6)

execute_process(COMMAND ${CMAKE_COMMAND}   -E create_symlink ../016_task2es/output/ ${CMAKE_CURRENT_BINARY_DIR}/input)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/input DESTINATION /opt/dkb/017_adjustMetadata )
install(DIRECTORY ./  DESTINATION /opt/dkb/017_adjustMetadata REGEX "input" EXCLUDE)
