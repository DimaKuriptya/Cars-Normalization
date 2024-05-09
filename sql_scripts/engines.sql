IF NOT EXISTS (SELECT 1 FROM [normalized].[Engine])
BEGIN
    SET IDENTITY_INSERT [normalized].[Engine] ON
    INSERT INTO [normalized].[Engine] (id, type) VALUES
    (0, 'gasoline'),
    (1, 'diesel'),
    (2, 'electric'),
    (3, 'gas');
    SET IDENTITY_INSERT [normalized].[Engine] OFF
END