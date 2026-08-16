if(ICEBERG_VANE_DISTRIBUTED)
  add_library(iceberg_distributed_write_objects OBJECT
              iceberg_distributed_write.cpp)
  set(ALL_OBJECT_FILES
      ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:iceberg_distributed_write_objects>
      PARENT_SCOPE)
endif()
