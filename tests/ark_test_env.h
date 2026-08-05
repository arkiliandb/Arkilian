/* tests/ark_test_env.h — portable setenv/unsetenv shim for test programs.
   POSIX exposes setenv()/unsetenv() via <stdlib.h>; MinGW-w64 does not.
   The Windows CRT provides _putenv("NAME=value") to set and _putenv("NAME")
   (no `=`) to remove an entry, which makes getenv() return NULL — matching
   unsetenv() semantics. Both are wrapped so test files can call
   ark_setenv()/ark_unsetenv() uniformly across Linux/macOS/Windows CI legs.   */
#ifndef ARK_TEST_ENV_H
#define ARK_TEST_ENV_H

#include <stdlib.h>

#ifdef _WIN32
#include <stdio.h>

static inline int ark_setenv(const char *name, const char *val, int overwrite) {
    char buf[1024];
    int n = snprintf(buf, sizeof(buf), "%s=%s", name, val);
    if (n < 0 || (size_t)n >= sizeof(buf)) return -1;
    (void)overwrite;        /* Windows has no overwrite semantics in _putenv */
    return _putenv(buf);
}

static inline int ark_unsetenv(const char *name) {
    /* _putenv with a bare name (no `=`) deletes the entry -> getenv() == NULL */
    return _putenv(name);
}

#else   /* POSIX: getenv/setenv are always present */

static inline int ark_setenv(const char *name, const char *val, int overwrite) {
    return setenv(name, val, overwrite);
}

static inline int ark_unsetenv(const char *name) {
    return unsetenv(name);
}

#endif /* _WIN32 */

#endif /* ARK_TEST_ENV_H */
