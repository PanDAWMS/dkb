cmake_minimum_required (VERSION 2.6)

project(dkb)

include(cmake/dkbVersions.cmake)

set(COMPILE_PACKAGING "1")


# Packaging step

if (${COMPILE_PACKAGING} STREQUAL "1")
  include(cmake/UseRPMToolsEnvironment.cmake)
  set(CPACK_SOURCE_PACKAGE_FILE_NAME
   "${PROJECT_NAME}-${DKB_VERSION}-${DKB_RELEASE}")
  message (STATUS
    "Setting package file name to: ${CPACK_SOURCE_PACKAGE_FILE_NAME}")
  set(CPACK_SOURCE_IGNORE_FILES "/.git/")
  include(CPack)
  include(cmake/UseRPMTools.cmake)
  if (RPMTools_FOUND)
    RPMTools_ADD_RPM_TARGETS(
      ${PROJECT_NAME} ${PROJECT_NAME}.spec.in)
  endif (RPMTools_FOUND)
endif (${COMPILE_PACKAGING} STREQUAL "1")

add_subdirectory(Utils/Dataflow/pyDKB)
add_subdirectory(Utils/Dataflow/test/pyDKB)

add_subdirectory(Utils/Dataflow/009_oracleConnector)
add_subdirectory(Utils/Dataflow/016_task2es)
add_subdirectory(Utils/Dataflow/017_adjustMetadata)
add_subdirectory(Utils/Dataflow/019_esFormat)
add_subdirectory(Utils/Dataflow/025_chicagoES)
add_subdirectory(Utils/Dataflow/069_upload2es)
add_subdirectory(Utils/Dataflow/091_datasetsRucio)
add_subdirectory(Utils/Dataflow/093_datasetsFormat)
add_subdirectory(Utils/Dataflow/095_datasetInfoAMI)

add_subdirectory(Utils/Dataflow/run)

add_custom_target(unittests
  echo "Testing 009_oracleConnector"
  COMMAND python -m unittest discover /opt/dkb/009_oracleConnector
  COMMAND echo "Testing 016_task2es"
  COMMAND python -m unittest discover /opt/dkb/016_task2es
  COMMAND echo "Testing 017_adjustMetadata"
  COMMAND python -m unittest discover /opt/dkb/017_adjustMetadata
  COMMAND echo "Testing 019_esFormat"
  COMMAND python -m unittest discover /opt/dkb/019_esFormat
  COMMAND echo "Testing 025_chicagoES"
  COMMAND python -m unittest discover /opt/dkb/025_chicagoES
  COMMAND echo "Testing 069_upload2es"
  COMMAND python -m unittest discover /opt/dkb/069_upload2es
  COMMAND echo "Testing 091_datasetsRucio"
  COMMAND python -m unittest discover /opt/dkb/091_datasetsRucio
  COMMAND echo "Testing 093_datasetsFormat"
  COMMAND python -m unittest discover /opt/dkb/093_datasetsFormat
  COMMAND echo "Testing 095_datasetInfoAMI"
  COMMAND python -m unittest discover /opt/dkb/095_datasetInfoAMI
  COMMAND echo "Testing pyDKB"
  COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/Utils/Dataflow/test/pyDKB/test.sh
  COMMENT "Running unit tests" VERBATIM)

