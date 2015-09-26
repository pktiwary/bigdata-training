<%@ page language="java" contentType="text/html; charset=UTF-8"
    pageEncoding="UTF-8" import="java.util.*" %>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<title>Insert title here</title>
</head>
<body>

<h1>HBase Access</h1>
<form method="get" action="HBaseServlet">
<%
out.println("HBase Table<input type=\"text\" name=\"hbaseTableName\" value=\"" + request.getParameter("hbaseTableName") + "\">");
out.println("Column family<input type=\"text\" name=\"columnFamily\" value=\"" + request.getParameter("columnFamily") + "\">");
out.println("Columns (comma separated)<input type=\"text\" name=\"columns\" value=\"" + request.getParameter("columns") + "\">");
%>
<input type="SUBMIT">
<p>
<%
out.println("<center>" + ((ArrayList<String[]>)request.getAttribute("result")).size() + " results found</center>");
%>
<table>
<%
ArrayList<String[]> result = (ArrayList<String[]>) request.getAttribute("result");
for(String[] columns: result) {
  out.println("<tr>");
  for(String column: columns) {
	  out.println("<td>" + column + "</td>");
  }
  out.println("</tr>");
}
%>
</table>
</form>

</body>
</html>