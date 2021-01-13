#include "../include/db.hpp"
#include <iostream>
#include <mutex>
#include <vector>
#include <atomic>

FILE *log_file = fopen("./performance.log", "w");
char *random_str(unsigned int size) {
  char *str = (char *)malloc(size + 1);
  for (unsigned int i = 0; i < size; i++) {
    switch (rand() % 3) {
      case 0:
        str[i] = rand() % 10 + '0';
        break;
      case 1:
        str[i] = rand() % 26 + 'A';
        break;
      case 2:
        str[i] = rand() % 26 + 'a';
        break;
      default:
        break;
    }
  }
  str[size] = 0;

  return str;
}

void test1() {
  DB *db = nullptr;
  DB::CreateOrOpen("./tmp", &db);
  Slice k;
  k.size() = 16;
  Slice v;

  int times = 10000;
  while (times--) {
    k.data() = random_str(16);
    v.size() = random() % 944 + 80;
    v.data() = random_str(v.size());
    printf("key %s\n", k.data());
    printf("val %s\n", v.data());
    db->Set(k, v);
    std::string a;
    db->Get(k, &a);
    printf("val %s\n", a.data());
    free(k.data());
    free(v.data());
  }

  char *t = "X112S0q592Qa56uC";
  Slice slice(t, 16);

  std::string a;
  db->Get(slice, &a);
  printf("key %s\n", slice.data());
  printf("key %s\n", a.data());
}

void test2() {
  DB *db = nullptr;
  DB::CreateOrOpen("./tmp", &db, log_file);
  Slice k;
  k.size() = 16;
  Slice v;
  v.size() = 80;
  int times = 1000;
  k.data() = random_str(16);
  v.data() = random_str(80);
  printf("old key %s\n", k.data());
  printf("old val %s\n", v.data());
  db->Set(k, v);
  std::string a;
  db->Get(k, &a);
  printf("get old val %s\n", a.data());

  Slice v2;
  v2.size() = 250;
  v2.data() = random_str(250);
  printf("new val %s\n", v2.data());
  db->Set(k, v2);

  std::string b;
  db->Get(k, &b);
  printf("get new val %s\n", b.data());

  Slice v3;
  v3.size() = 240;
  v3.data() = random_str(240);
  printf("new val %s\n", v3.data());
  db->Set(k, v3);
  std::string c;
  db->Get(k, &c);
  printf("get new val %s\n", c.data());

  free(k.data());
  free(v.data());
  free(v2.data());
  free(v3.data());
}

void test3() {
  DB *db = nullptr;
  DB::CreateOrOpen("/mnt/pmem/DB", &db, log_file);
  Slice k;
  k.size() = 16;
  Slice v;

  int times = 1000000;
  std::vector<Slice> compare;

  char *target = "157qCvn65N7ns5n0";

  while (times--) {
    k.data() = random_str(16);
    v.size() = random() % 944 + 80;
    v.data() = random_str(v.size());

    db->Set(k, v);
   /* if (0 == strcmp(k.data(), target)) {
      Slice slice(target);
      std::string a;

      db->Get(slice, &a);
      printf("set size %d\n", v.size());
      printf("get size %d\n", a.size());
    }
    compare.push_back(k);
    compare.push_back(v);*/
  }
  /*Slice slice(target);
  std::string a;
  db->Get(slice, &a);
  int count = 0;
  for (int i = 0; i < compare.size(); i += 2) {
    std::string a;
    db->Get(compare.at(i), &a);
    if (0 == strcmp(a.data(), compare.at(i + 1).data())) {
      count++;
    } else {
      printf("set key %s \n", compare.at(i).data());
      printf("set val %s size %d\n", compare.at(i + 1).data(), compare.at(i + 1).size());
      printf("get val %s size %d\n", a.data(), a.size());
    }
  }

  std::cout << "sum: " << compare.size() / 2 << "right: " << count
            << "wrong: " << compare.size() / 2 - count << std::endl;*/
}

void test4() {
  char *key = "X112S0q592Qa56uC";
  Slice slice_key(key, 60);
  char *val1 = "qwer";
  char *val2 = "asdf";
  Slice slice_v1(val1, 4);
  Slice slice_v2(val2, 4);
  DB *db = nullptr;
  DB::CreateOrOpen("./tmp", &db, log_file);
  db->Set(slice_key, slice_v1);
  db->Set(slice_key, slice_v2);
  std::string b;
  db->Get(slice_key, &b);
  std::cout << b << std::endl;
}
int main() { test3(); }
