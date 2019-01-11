#pragma once

/*******************************************************************************

SIMPLE C++ WRAPPER FOR LIBMYSQLCLIENT and LIBMARIADB.

Copyright (c) 2018-2019, Ivan Pizhenko. All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice,
this list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation and/or
other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its contributors
may be used to endorse or promote products derived from this software without
specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

*******************************************************************************/

#ifndef LIBMYSQLCLIENT_CPP_WRAPPER_H__
#define LIBMYSQLCLIENT_CPP_WRAPPER_H__

#include <mysql.h>
#include <cstring>
#include <memory>
#include <stdexcept>
#include <sstream>
#include <vector>

namespace mysql_cpp_wrapper {

	namespace Version {
		constexpr int MAJOR = 0;
		constexpr int MINOR = 0;
		constexpr int PATCH = 0;
	}

	class MySQLClientLibrary;
	class MySQLConnection;
	class MySQLPreparedStatement;

	class MySQLClientLibrary {
	private:
		MySQLClientLibrary(int argc, char** argv, char** groups) :
			m_errorCode(mysql_library_init(argc, argv, groups))
		{
			if (m_errorCode != 0) {
				throw std::runtime_error("failed to initialize MySQL client library");
			}
		}

	public:

		static std::shared_ptr<MySQLClientLibrary> createOrGet(int argc = 0, char** argv = nullptr,
			char** groups = nullptr)
		{
			auto instance = m_instance.lock();
			if (!instance) {
				instance = std::shared_ptr<MySQLClientLibrary>(new MySQLClientLibrary(argc, argv, groups));
				m_instance = instance;
			}
			return instance;
		}

		~MySQLClientLibrary()
		{
			if (m_errorCode == 0) {
				mysql_library_end();
			}
		}

		MySQLClientLibrary(const MySQLClientLibrary&) = delete;
		MySQLClientLibrary(MySQLClientLibrary&&) = delete;
		MySQLClientLibrary& operator=(const MySQLClientLibrary&) = delete;
		MySQLClientLibrary& operator=(MySQLClientLibrary&&) = delete;

	private:
		const int m_errorCode;
		static std::weak_ptr<MySQLClientLibrary> m_instance;
	};

	class MySQLConnection {
	private:
		MySQLConnection(const std::shared_ptr<MySQLClientLibrary>& library) :
			m_library(library),
			m_mysql(mysql_init(nullptr))
		{
			if (!m_mysql) {
				throw std::runtime_error("failed to initialize connection object");
			}
		}

	public:

		static std::shared_ptr<MySQLConnection> create(
			const std::shared_ptr<MySQLClientLibrary>& library)
		{
			return std::shared_ptr<MySQLConnection>(new MySQLConnection(library));
		}

		~MySQLConnection()
		{
			if (m_mysql) {
				mysql_close(m_mysql);
			}
		}

		MySQLConnection(const MySQLConnection&) = delete;
		MySQLConnection(MySQLConnection&&) = delete;
		MySQLConnection& operator=(const MySQLConnection&) = delete;
		MySQLConnection& operator=(MySQLConnection&&) = delete;

		operator MYSQL*() const noexcept
		{
			return m_mysql;
		}

		void connect(const std::string& host, const unsigned port, const std::string& database,
			const std::string& user, const char* password, unsigned long clientFlags = 0)
		{
			if (!mysql_real_connect(m_mysql, host.c_str(), user.c_str(), password,
				database.c_str(), port, nullptr, clientFlags)) {
				throw std::runtime_error("could not connect to database server");
			}
		}

		void setAutoCommit(bool autoCommit = true)
		{
			if (mysql_autocommit(m_mysql, autoCommit ? 1 : 0)) {
				std::ostringstream err;
				err << "could not set autocommit mode to " << (autoCommit ? "on" : "off");
				throw std::runtime_error(err.str());
			}
		}

		unsigned long getServerVersion() const noexcept
		{
			return mysql_get_server_version(m_mysql);
		}

	private:
		std::shared_ptr<MySQLClientLibrary> m_library;
		::MYSQL* m_mysql;
	};

	class MySQLPreparedStatement {
	private:
		MySQLPreparedStatement(const std::shared_ptr<MySQLConnection>& conn) :
			m_conn(conn),
			m_stmt(mysql_stmt_init((MYSQL*)*conn)),
			m_parameters(),
			m_results(),
			m_errorCode(0)
		{
			if (!m_stmt) {
				throw std::runtime_error("failed to initialize MySQL prepared statement object");
			}
		}

	public:

		static std::shared_ptr<MySQLPreparedStatement> create(
			const std::shared_ptr<MySQLConnection>& conn)
		{
			return std::shared_ptr<MySQLPreparedStatement>(new MySQLPreparedStatement(conn));
		}

		~MySQLPreparedStatement()
		{
			if (m_stmt) {
				mysql_stmt_close(m_stmt);
			}
		}

		MySQLPreparedStatement(const MySQLPreparedStatement&) = delete;
		MySQLPreparedStatement(MySQLPreparedStatement&&) = delete;
		MySQLPreparedStatement& operator=(const MySQLPreparedStatement&) = delete;
		MySQLPreparedStatement& operator=(MySQLPreparedStatement&&) = delete;

		operator MYSQL_STMT*() const noexcept
		{
			return m_stmt;
		}

		int getErrorCode() const noexcept
		{
			return m_errorCode;
		}

		void prepare(const std::string& sql)
		{
			prepare(sql.c_str(), sql.length());
		}

		void prepare(const char* sql, std::size_t len = std::string::npos)
		{
			if (len == std::string::npos) {
				len = strlen(sql);
			}
			m_errorCode = mysql_stmt_prepare(m_stmt, sql, static_cast<unsigned long>(len));
			if (m_errorCode != 0) {
				std::ostringstream err;
				err << "failed to prepare prepared statement: "
					<< mysql_stmt_error(m_stmt);
				throw std::runtime_error(err.str());
			}
		}

		void addParameter(enum enum_field_types bufferType, const void* buffer,
				std::size_t bufferLength)
		{
			MYSQL_BIND parameter;
			memset(&parameter, 0, sizeof(parameter));
			parameter.buffer_type = bufferType;
			parameter.buffer = const_cast<void*>(buffer);
			parameter.buffer_length = static_cast<unsigned long>(bufferLength);
			m_parameters.push_back(parameter);
		}

		void addParameter(const void* buffer, unsigned long bufferLength)
		{
			addParameter(MYSQL_TYPE_BLOB, buffer, bufferLength);
		}

		void addParameter(const char& value)
		{
			addParameter(MYSQL_TYPE_TINY, &value, sizeof(value));
		}

		void addParameter(const short& value)
		{
			addParameter(MYSQL_TYPE_SHORT, &value, sizeof(value));
		}

		void addParameter(const int& value)
		{
			addParameter(MYSQL_TYPE_LONG, &value, sizeof(value));
		}

		void addParameter(const long long& value)
		{
			addParameter(MYSQL_TYPE_LONGLONG, &value, sizeof(value));
		}

		void addParameter(const float& value)
		{
			addParameter(MYSQL_TYPE_FLOAT, &value, sizeof(value));
		}

		void addParameter(const double& value)
		{
			addParameter(MYSQL_TYPE_FLOAT, &value, sizeof(value));
		}

		void addParameter(const char* value, std::size_t size)
		{
			addParameter(MYSQL_TYPE_STRING, value, size);
		}

		void setParameterLength(std::size_t index, std::size_t length)
		{
			m_parameters.at(index).buffer_length = static_cast<unsigned long>(length);
		}

		void bindParameters()
		{
			if (m_parameters.empty()) {
				throw std::logic_error("there are no parameters");
			}
			m_errorCode = mysql_stmt_bind_param(m_stmt, &m_parameters[0]);
			if (m_errorCode != 0) {
				std::ostringstream err;
				err << "failed to bind parameters prepared statement: "
					<< mysql_stmt_error(m_stmt);
				throw std::runtime_error(err.str());
			}
		}

		void addResult(enum enum_field_types bufferType, void* buffer,
			std::size_t bufferLength)
		{
			MYSQL_BIND parameter;
			memset(&parameter, 0, sizeof(parameter));
			parameter.buffer_type = bufferType;
			parameter.buffer = buffer;
			parameter.buffer_length = static_cast<unsigned long>(bufferLength);
			m_results.push_back(parameter);
		}

		void addResult(void* buffer, std::size_t bufferLength)
		{
			addResult(MYSQL_TYPE_BLOB, buffer, bufferLength);
		}

		void addResult(char& value)
		{
			addResult(MYSQL_TYPE_TINY, &value, sizeof(value));
		}

		void addResult(short& value)
		{
			addResult(MYSQL_TYPE_SHORT, &value, sizeof(value));
		}

		void addResult(int& value)
		{
			addResult(MYSQL_TYPE_LONG, &value, sizeof(value));
		}

		void addResult(long long& value)
		{
			addResult(MYSQL_TYPE_LONGLONG, &value, sizeof(value));
		}

		void addResult(float& value)
		{
			addResult(MYSQL_TYPE_FLOAT, &value, sizeof(value));
		}

		void addResult(double& value)
		{
			addResult(MYSQL_TYPE_DOUBLE, &value, sizeof(value));
		}

		void addResult(char* value, std::size_t size)
		{
			addResult(MYSQL_TYPE_STRING, value, size);
		}

		void bindResults()
		{
			if (m_results.empty()) {
				throw std::logic_error("there are no results");
			}
			m_errorCode = mysql_stmt_bind_result(m_stmt, &m_results[0]);
			if (m_errorCode != 0) {
				std::ostringstream err;
				err << "failed to bind results prepared statement: "
					<< mysql_stmt_error(m_stmt);
				throw std::runtime_error(err.str());
			}
		}

		void execute()
		{
			m_errorCode = mysql_stmt_execute(m_stmt);
			if (m_errorCode != 0) {
				std::ostringstream err;
				err << "failed to execute prepared statement: "
					<< mysql_stmt_error(m_stmt);
				throw std::runtime_error(err.str());
			}
		}

		bool fetch()
		{
			const int res = mysql_stmt_fetch(m_stmt);
			switch (res) {
			case 0: return true;
			case MYSQL_NO_DATA: return false;
			default: throw std::runtime_error("failed to fetch data");
			}
		}

		void stop()
		{
			if (mysql_stmt_free_result(m_stmt)) {
				throw std::runtime_error("failed to stop prepared statement");
			}
		}

	private:
		std::shared_ptr<MySQLConnection> m_conn;
		MYSQL_STMT* m_stmt;
		std::vector<MYSQL_BIND> m_parameters;
		std::vector<MYSQL_BIND> m_results;
		int m_errorCode;
	};

} // namespace mysql_cpp_wrapper

#endif // LIBMYSQLCLIENT_CPP_WRAPPER_H__
