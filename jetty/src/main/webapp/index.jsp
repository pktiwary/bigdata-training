<html>
<body>
<h1>HBase Access</h1>
<form method="get" action="HBaseServlet">
HBase Table<input type="text" name="hbaseTableName">
Column family<input type="text" name="columnFamily">
Columns (comma separated)<input type="text" name="columns">
<input type="submit" name="action" value="Query">
<input type="submit" name="action" value="Create">
</form>
</body>
</html>
