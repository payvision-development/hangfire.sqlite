﻿BEGIN TRANSACTION;

-- Create the Schema table if not exists
CREATE TABLE IF NOT EXISTS [Schema](
    [Version] INTEGER NOT NULL PRIMARY KEY
);

-- Create Job tables
CREATE TABLE IF NOT EXISTS [Job] (
    [Id]				INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    [StateId]			BIGINT,
    [StateName]			NVARCHAR(20) COLLATE NOCASE,
    [InvocationData]	TEXT NOT NULL COLLATE NOCASE,
    [Arguments]			TEXT NOT NULL COLLATE NOCASE,
    [CreatedAt]			DATETIME NOT NULL,
    [ExpireAt]			DATETIME
);
CREATE INDEX IF NOT EXISTS [Job_IX_HangFire_Job_ExpireAt]
ON [Job]
([ExpireAt] ASC);
CREATE INDEX IF NOT EXISTS [IX_HangFire_Job_StateName]
ON [Job]
([StateName] ASC);

CREATE TABLE IF NOT EXISTS [JobParameter] (
    [JobId] INTEGER NOT NULL,
    [Name]  NVARCHAR(40) NOT NULL COLLATE NOCASE,
    [Value] TEXT COLLATE NOCASE
,
    FOREIGN KEY ([JobId])
        REFERENCES [Job]([Id])
        ON UPDATE CASCADE
        ON DELETE CASCADE
);
CREATE UNIQUE INDEX IF NOT EXISTS [PK_HangFire_JobParameter]
ON [JobParameter]
([JobId] ASC, [Name] ASC);

CREATE TABLE IF NOT EXISTS [State] (
    [Id]		INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    [JobId]		INTEGER NOT NULL,
    [Name]		NVARCHAR(20) NOT NULL COLLATE NOCASE,
    [Reason]    NVARCHAR(100) COLLATE NOCASE,
    [CreatedAt] DATETIME NOT NULL,
    [Data]		TEXT COLLATE NOCASE
,
    FOREIGN KEY ([JobId])
        REFERENCES [Job]([Id])
        ON UPDATE CASCADE
        ON DELETE CASCADE
);
CREATE UNIQUE INDEX IF NOT EXISTS [PK_HangFire_State]
ON [State]
([JobId] ASC, [Id] ASC);

-- Create Job queue table
CREATE TABLE IF NOT EXISTS [JobQueue] (
    [Id]		INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    [JobId]		BIGINT NOT NULL,
    [Queue]		NVARCHAR(50) NOT NULL COLLATE NOCASE,
    [FetchedAt] DATETIME,
    [LockedBy]	TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS [PK_HangFire_JobQueue]
ON [JobQueue]
([Id] ASC, [Queue] ASC);
CREATE INDEX IF NOT EXISTS [JobQueue_LockedBy]
ON [JobQueue]
([LockedBy] DESC);

-- Create Server tables
CREATE TABLE IF NOT EXISTS [Server] (
    [Id]			NVARCHAR(100) PRIMARY KEY NOT NULL COLLATE NOCASE,
    [Data]			TEXT COLLATE NOCASE,
    [LastHeartbeat]	DATETIME NOT NULL
);
CREATE INDEX IF NOT EXISTS [IX_HangFire_Server_LastHeartbeat]
ON [Server]
([LastHeartbeat] ASC);

-- Extension tables
CREATE TABLE IF NOT EXISTS [Set] (
	[Id]		INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
	[Key]		NVARCHAR(100) NOT NULL COLLATE NOCASE,
	[Score]		FLOAT NOT NULL,
	[Value]		NVARCHAR(256) NOT NULL COLLATE NOCASE,
	[ExpireAt]	DATETIME
);

CREATE UNIQUE INDEX IF NOT EXISTS [PK_HangFire_Set]
ON [Set]
([Key] ASC, [VALUE] ASC);
CREATE INDEX IF NOT EXISTS [IX_HangFire_Set_ExpireAt]
ON [Set]
([ExpireAt] ASC);
CREATE INDEX IF NOT EXISTS [IX_HangFire_Set_Score]
ON [Set]
([Key] ASC, [Score] ASC);

--SET SCHEMA VERSION
REPLACE INTO [Schema]([Version]) VALUES (1);

COMMIT TRANSACTION;