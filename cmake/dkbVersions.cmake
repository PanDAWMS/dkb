# Default dkb Version
set(DKB_VERSION 0)
set(DKB_RELEASE 1)

# Change the release number if VCS version is provided
if(DEFINED VCS_VERSION)
  set(DKB_RELEASE ${VCS_VERSION})
  message(STATUS "Replaced DKB_RELEASE with VCS_VERSION: ${DKB_RELEASE}")
endif(DEFINED VCS_VERSION)

message(STATUS "DKB version is ${DKB_VERSION}-${DKB_RELEASE}")

