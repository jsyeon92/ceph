// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <boost/program_options/variables_map.hpp>
#include <boost/program_options/parsers.hpp>

#include <stdio.h>
#include <string.h>
#include <iostream>
#include <time.h>
#include <fcntl.h>
#include "common/ceph_argparse.h"
#include "include/stringify.h"
#include "common/errno.h"
#include "common/safe_io.h"

#include "global/global_init.h"
#include "os/bluestore/BlueFS.h"
#include "os/bluestore/BlueStore.h"
#include "os/bluestore/BlueRocksEnv.h"
#include "common/admin_socket.h"

#include "rocksdb/db.h"
#include "rocksdb/table.h"
#include "rocksdb/env.h"
#include "rocksdb/slice.h"
#include "rocksdb/cache.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/utilities/convenience.h"
#include "rocksdb/util/stderr_logger.h"
#include "rocksdb/merge_operator.h"
#include "kv/rocksdb_cache/BinnedLRUCache.h"
#include "rocksdb/db_bench_tool.h"

namespace po = boost::program_options;

void usage(po::options_description &desc)
{
  cout << desc << std::endl;
}

void validate_path(CephContext *cct, const string& path, bool bluefs)
{
  BlueStore bluestore(cct, path);
  string type;
  int r = bluestore.read_meta("type", &type);
  if (r < 0) {
    cerr << "failed to load os-type: " << cpp_strerror(r) << std::endl;
    exit(EXIT_FAILURE);
  }
  if (type != "bluestore") {
    cerr << "expected bluestore, but type is " << type << std::endl;
    exit(EXIT_FAILURE);
  }
  if (!bluefs) {
    return;
  }

  string kv_backend;
  r = bluestore.read_meta("kv_backend", &kv_backend);
  if (r < 0) {
    cerr << "failed to load kv_backend: " << cpp_strerror(r) << std::endl;
    exit(EXIT_FAILURE);
  }
  if (kv_backend != "rocksdb") {
    cerr << "expect kv_backend to be rocksdb, but is " << kv_backend
         << std::endl;
    exit(EXIT_FAILURE);
  }
  string bluefs_enabled;
  r = bluestore.read_meta("bluefs", &bluefs_enabled);
  if (r < 0) {
    cerr << "failed to load do_bluefs: " << cpp_strerror(r) << std::endl;
    exit(EXIT_FAILURE);
  }
  if (bluefs_enabled != "1") {
    cerr << "bluefs not enabled for rocksdb" << std::endl;
    exit(EXIT_FAILURE);
  }
}

const char* find_device_path(
  int id,
  CephContext *cct,
  const vector<string>& devs)
{
  for (auto& i : devs) {
    bluestore_bdev_label_t label;
    int r = BlueStore::_read_bdev_label(cct, i, &label);
    if (r < 0) {
      cerr << "unable to read label for " << i << ": "
	   << cpp_strerror(r) << std::endl;
      exit(EXIT_FAILURE);
    }
    if ((id == BlueFS::BDEV_SLOW && label.description == "main") ||
        (id == BlueFS::BDEV_DB && label.description == "bluefs db") ||
        (id == BlueFS::BDEV_WAL && label.description == "bluefs wal")) {
      return i.c_str();
    }
  }
  return nullptr;
}

void parse_devices(
  CephContext *cct,
  const vector<string>& devs,
  map<string, int>* got,
  bool* has_db,
  bool* has_wal)
{
  string main;
  bool was_db = false;
  if (has_wal) {
    *has_wal = false;
  }
  if (has_db) {
    *has_db = false;
  }
  for (auto& d : devs) {
    bluestore_bdev_label_t label;
    int r = BlueStore::_read_bdev_label(cct, d, &label);
    if (r < 0) {
      cerr << "unable to read label for " << d << ": "
	   << cpp_strerror(r) << std::endl;
      exit(EXIT_FAILURE);
    }
    int id = -1;
    if (label.description == "main")
      main = d;
    else if (label.description == "bluefs db") {
      id = BlueFS::BDEV_DB;
      was_db = true;
      if (has_db) {
	*has_db = true;
      }
    }
    else if (label.description == "bluefs wal") {
      id = BlueFS::BDEV_WAL;
      if (has_wal) {
	*has_wal = true;
      }
    }
    if (id >= 0) {
      got->emplace(d, id);
    }
  }
  if (main.length()) {
    int id = was_db ? BlueFS::BDEV_SLOW : BlueFS::BDEV_DB;
    got->emplace(main, id);
  }
}

void add_devices(
  BlueFS *fs,
  CephContext *cct,
  const vector<string>& devs)
{
  map<string, int> got;
  parse_devices(cct, devs, &got, nullptr, nullptr);
  for(auto e : got) {
    char target_path[PATH_MAX] = "";
    if(!e.first.empty()) {
      if (realpath(e.first.c_str(), target_path) == nullptr) {
	cerr << "failed to retrieve absolute path for " << e.first
	      << ": " << cpp_strerror(errno)
	      << std::endl;
      }
    }

    cout << " slot " << e.second << " " << e.first;
    if (target_path[0]) {
      cout << " -> " << target_path;
    }
    cout << std::endl;
    int r = fs->add_block_device(e.second, e.first, false);
    if (r < 0) {
      cerr << "unable to open " << e.first << ": " << cpp_strerror(r) << std::endl;
      exit(EXIT_FAILURE);
    }
  }
}

BlueFS *open_bluefs(
  CephContext *cct,
  const string& path,
  const vector<string>& devs)
{
  validate_path(cct, path, true);
  BlueFS *fs = new BlueFS(cct);

  add_devices(fs, cct, devs);

  int r = fs->mount();
  if (r < 0) {
    cerr << "unable to mount bluefs: " << cpp_strerror(r)
	 << std::endl;
    exit(EXIT_FAILURE);
  }
  return fs;
}

void log_dump(
  CephContext *cct,
  const string& path,
  const vector<string>& devs)
{
  BlueFS* fs = open_bluefs(cct, path, devs);
  int r = fs->log_dump();
  if (r < 0) {
    cerr << "log_dump failed" << ": "
         << cpp_strerror(r) << std::endl;
    exit(EXIT_FAILURE);
  }

  delete fs;
}

void inferring_bluefs_devices(vector<string>& devs, std::string& path)
{
  cout << "inferring bluefs devices from bluestore path" << std::endl;
  for (auto fn : {"block", "block.wal", "block.db"}) {
    string p = path + "/" + fn;
    struct stat st;
    if (::stat(p.c_str(), &st) == 0) {
      devs.push_back(p);
    }
  }
}

int main(int argc, char **argv)
{
  vector<string> devs;
  string path="/var/lib/ceph/osd/ceph-55";
  string action;

  action="bluefs-db-bench";

  vector<const char*> args;
  args.push_back("--no-log-to-stderr");
  args.push_back("--err-to-stderr");
  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
			 CODE_ENVIRONMENT_UTILITY,
			 CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);

  common_init_finish(cct.get());

  inferring_bluefs_devices(devs, path);
  rocksdb::Env *env = nullptr;
  BlueFS *bluefs = nullptr;

  bluefs = open_bluefs(cct.get(), path, devs);
  env = new BlueRocksEnv(bluefs);
  rocksdb::db_bench_tool(argc, argv, env);
  bluefs->umount();
  delete bluefs;

  return 0;
}
