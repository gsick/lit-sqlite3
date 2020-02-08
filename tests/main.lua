local sqlite = require("sqlite3")
local Path   = require("path")
local Fs     = require("fs")

-- remove temp db
if Fs.accessSync(Path.join(process.cwd(), "tmp/test.sqlite")) then
  Fs.unlinkSync(Path.join(process.cwd(), "tmp/test.sqlite"))
end

-- create a DB
local db, err = sqlite.open("tmp/test.sqlite", "read")
if not db then
  assert(err == "unable to open database file")
end

-- create a DB
local db, err = sqlite.open("/root/test.sqlite", "create")
if not db then
  assert(err == "unable to open database file")
end

-- create a DB
local db, err = sqlite.open("tmp/test.sqlite", "create")
if not db then
  p(err)
end
assert(db ~= nil)

-- create a table
local users_table_sql = [[
  PRAGMA foreign_keys = ON;
  CREATE TABLE IF NOT EXISTS users (
    uuid TEXT PRIMARY KEY,
    login TEXT,
    password TEXT
  );
]]

local users2_table_sql = [[
  CREATE TABLE users (
    uuid TEXT PRIMARY KEY,
    login TEXT,
    password TEXT
  );
]]

local r, err = db:exec(users_table_sql)
if r ~= sqlite.OK then
  p(err)
end
assert(r == sqlite.OK)

local r, err = db:exec(users2_table_sql)
if r ~= sqlite.OK then
  assert(err == "table users already exists")
end

local r, err = db:close()
if r ~= sqlite.OK then
  p(err)
end
assert(r == sqlite.OK)

-- open a DB
local db, err = sqlite.open("tmp/test.sqlite", "write")
if not db then
  p(err)
end
assert(db ~= nil)

local users_insert_sql = [[
  INSERT INTO users VALUES(?,?,?);
]]

local stmt, err = db:prepare(users_insert_sql)
if not stmt then
  p(err)
end
assert(stmt ~= nil)


local data = {
  {
    "uuid-1",
    "foo",
    "bar"
  },
  {
    "uuid-2",
    "foo2",
    "bar2"
  }
}


local r = true
local err

local i = 1
while r and i <= #data do

  local a, b = stmt:bind_values(unpack(data[i]))

  if a ~= sqlite.OK then
    p(b)
  end
  assert(a == sqlite.OK)

  r, err = stmt:step()
  if r ~= true then
    stmt:reset()
    r = true
  end

  i=i+1
end
if r == nil then
  p(err)
end

local r, err = stmt:finalize()
if r ~= sqlite.OK then
  p(err)
end

local stmt, err = db:prepare(users_insert_sql)
assert(stmt ~= nil)

stmt:bind_values(
  "uuid-1",
  "foo",
  "bar"
)

local r, err = stmt:step()
if err then
  -- already exists
  assert(r == sqlite.CONSTRAINT)
  assert(err == "UNIQUE constraint failed: users.uuid")
end
local r, err = stmt:finalize()
if err then
  -- already exists
  assert(r == sqlite.CONSTRAINT)
  assert(err == "UNIQUE constraint failed: users.uuid")
end

local r, err = db:close()
if r ~= sqlite.OK then
  p(err)
end

local db, err = sqlite.open("tmp/test.sqlite", "read")

local stmt, err = db:prepare("SELECT uuid FROM users WHERE login=? AND password=? LIMIT 1")
assert(stmt ~= nil)

local r, err = stmt:bind_values(
  "foo",
  "bar"
)

local row = stmt:rows()
if row then

  local r = row()
  if r then
    assert(r[1] == "uuid-1")
  end
end

local r, err = stmt:finalize()

local stmt, err = db:prepare("SELECT uuid, login FROM users")
assert(stmt ~= nil)

local row = stmt:rows()
if row then

  local r = row()
  local i = 1
  while r do
    assert(r[1] == "uuid-" .. i)
    i = i + 1
    r = row()
  end
end

local r, err = stmt:finalize()

local stmt, err = db:prepare("PRAGMA foreign_keys")
if not stmt then
  p(err)
end
assert(stmt ~= nil)
local row = stmt:rows()
if row then
  p(row())
end

local r, err = db:close()
if r ~= sqlite.OK then
  p(err)
end
