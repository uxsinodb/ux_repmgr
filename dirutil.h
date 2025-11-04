/*
 * dirutil.h
 * Portions Copyright (c) 2016-2022, Beijing Uxsino Software Limited, Co.
 * Copyright (c) 2009-2020, UXDB Software Co.,Ltd.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef _DIRUTIL_H_
#define _DIRUTIL_H_

typedef enum
{
	DIR_ERROR = -1,
	DIR_NOENT,
	DIR_EMPTY,
	DIR_NOT_EMPTY
} DataDirState;

typedef enum
{
	UX_DIR_ERROR = -1,
	UX_DIR_NOT_RUNNING,
	UX_DIR_RUNNING
} UxDirState;

extern int	mkdir_p(char *path, mode_t omode);
extern bool set_dir_permissions(const char *path, int server_version_num);

extern DataDirState	check_dir(const char *path);
extern bool create_dir(const char *path);
extern bool is_ux_dir(const char *path);
extern UxDirState is_ux_running(const char *path);
extern bool create_ux_dir(const char *path, bool force);
extern int rmdir_recursive(const char *path);

#endif
