/*
 *
 * compat.c
 *	  Provides a couple of useful string utility functions adapted
 *	  from the backend code, which are not publicly exposed in all
 *    supported UXsinoDB versions. They're unlikely to change but
 *    it would be worth keeping an eye on them for any fixes/improvements.
 *
 * Portions Copyright (c) 2016-2022, Beijing Uxsino Software Limited, Co.
 * Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
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
 *
 */

#include "repmgr.h"
#include "compat.h"

/*
 * Append the given string to the buffer, with suitable quoting for passing
 * the string as a value, in a keyword/pair value in a libuxsql connection
 * string
 *
 * This function is adapted from src/fe_utils/string_utils.c (before 9.6
 * located in: src/bin/ux_dump/dumputils.c)
 */
void
appendConnStrVal(UXSQLExpBuffer buf, const char *str)
{
	const char *s;
	bool		needquotes;

	/*
	 * If the string is one or more plain ASCII characters, no need to quote
	 * it. This is quite conservative, but better safe than sorry.
	 */
	needquotes = true;
	for (s = str; *s; s++)
	{
		if (!((*s >= 'a' && *s <= 'z') || (*s >= 'A' && *s <= 'Z') ||
			  (*s >= '0' && *s <= '9') || *s == '_' || *s == '.'))
		{
			needquotes = true;
			break;
		}
		needquotes = false;
	}

	if (needquotes)
	{
		appendUXSQLExpBufferChar(buf, '\'');
		while (*str)
		{
			/* ' and \ must be escaped by to \' and \\ */
			if (*str == '\'' || *str == '\\')
				appendUXSQLExpBufferChar(buf, '\\');

			appendUXSQLExpBufferChar(buf, *str);
			str++;
		}
		appendUXSQLExpBufferChar(buf, '\'');
	}
	else
		appendUXSQLExpBufferStr(buf, str);
}

/*
 * Adapted from: src/fe_utils/string_utils.c
 */
void
appendShellString(UXSQLExpBuffer buf, const char *str)
{
	const char *p;

	appendUXSQLExpBufferChar(buf, '\'');
	for (p = str; *p; p++)
	{
		if (*p == '\n' || *p == '\r')
		{
			fprintf(stderr,
					_("shell command argument contains a newline or carriage return: \"%s\"\n"),
					str);
			exit(ERR_BAD_CONFIG);
		}

		if (*p == '\'')
			appendUXSQLExpBufferStr(buf, "'\"'\"'");
		else if (*p == '&')
			appendUXSQLExpBufferStr(buf, "\\&");
		else
			appendUXSQLExpBufferChar(buf, *p);
	}

	appendUXSQLExpBufferChar(buf, '\'');
}

/*
 * Adapted from: src/fe_utils/string_utils.c
 */
void
appendRemoteShellString(UXSQLExpBuffer buf, const char *str)
{
	const char *p;

	appendUXSQLExpBufferStr(buf, "\\'");

	for (p = str; *p; p++)
	{
		if (*p == '\n' || *p == '\r')
		{
			fprintf(stderr,
					_("shell command argument contains a newline or carriage return: \"%s\"\n"),
					str);
			exit(ERR_BAD_CONFIG);
		}

		if (*p == '\'')
			appendUXSQLExpBufferStr(buf, "'\"'\"'");
		else if (*p == '&')
			appendUXSQLExpBufferStr(buf, "\\&");
		else
			appendUXSQLExpBufferChar(buf, *p);
	}

	appendUXSQLExpBufferStr(buf, "\\'");
}
