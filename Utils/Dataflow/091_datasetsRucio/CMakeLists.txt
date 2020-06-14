cmake_minimum_required (VERSION 2.6)

execute_process(COMMAND ${CMAKE_COMMAND}   -E create_symlink ../009_oracleConnector/output/ ${CMAKE_CURRENT_BINARY_DIR}/input)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/input DESTINATION /opt/dkb/091_datasetsRucio )
install(DIRECTORY ./  DESTINATION /opt/dkb/091_datasetsRucio REGEX "input" EXCLUDE)

