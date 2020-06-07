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
