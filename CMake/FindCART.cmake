# - Try to find CART
# Once done this will define
#  CART_FOUND - System has CART
#  CART_INCLUDE_DIRS - The CART include directories
#  CART_LIBRARIES - The libraries needed to use CART

find_package(PkgConfig)
pkg_check_modules(PC_CART cart)

find_path(CART_INCLUDE_DIR cart/types.h
  HINTS ${PC_CART_INCLUDEDIR} ${PC_CART_INCLUDE_DIRS}
  PATHS /usr/local/include /usr/include)

find_library(CART_LIBRARY NAMES cart
  HINTS ${PC_CART_LIBDIR} ${PC_CART_LIBRARY_DIRS}
  PATHS /usr/local/lib64 /usr/local/lib /usr/lib64 /usr/lib)

set(CART_INCLUDE_DIRS ${CART_INCLUDE_DIR})
set(CART_LIBRARIES ${CART_LIBRARY})

include(FindPackageHandleStandardArgs)
# handle the QUIETLY and REQUIRED arguments and set CART_FOUND to TRUE
# if all listed variables are TRUE
find_package_handle_standard_args(CART DEFAULT_MSG
                                  CART_INCLUDE_DIR CART_LIBRARY)

mark_as_advanced(CART_INCLUDE_DIR CART_LIBRARY)

