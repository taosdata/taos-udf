cmake_minimum_required(VERSION 3.4...3.18)
project(${SKBUILD_PROJECT_NAME} VERSION ${SKBUILD_PROJECT_VERSION} LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_VISIBILITY_PRESET hidden)
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -fsanitize=address")

if (SKBUILD)
  find_package(pybind11 CONFIG REQUIRED)
else ()
  add_subdirectory(extern/pybind11)
endif()	

include_directories(extern/plog/include)

include_directories(src)

add_library(taospyudf SHARED src/taospyudf.cpp src/taospyudf.h)
target_link_libraries(taospyudf PUBLIC pybind11::embed)

if (SKBUILD)
  install(TARGETS taospyudf DESTINATION taospyudf)
endif ()

# Legacy convenience install for direct (non-pip) builds. Only meaningful
# on Unix-like systems where the libdir is on the default loader path.
# Windows users should install via ``pip install taospyudf`` (the wheel
# places the DLL inside the Python package directory).
# Honor CMAKE_INSTALL_PREFIX (default /usr/local) and the multiarch-aware
# libdir from GNUInstallDirs so distros / packagers can relocate the install.
if (NOT WIN32 AND NOT SKBUILD)
  include(GNUInstallDirs)
  install(TARGETS taospyudf DESTINATION ${CMAKE_INSTALL_LIBDIR})
endif ()
