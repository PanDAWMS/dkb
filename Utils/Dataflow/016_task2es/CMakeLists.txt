cmake_minimum_required (VERSION 2.6)

execute_process(COMMAND ${CMAKE_COMMAND}   -E create_symlink ../025_chicagoES/output/ ${CMAKE_CURRENT_BINARY_DIR}/input)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/input DESTINATION /opt/dkb/016_task2es)
install(DIRECTORY ./  DESTINATION /opt/dkb/016_task2es REGEX "input" EXCLUDE)

