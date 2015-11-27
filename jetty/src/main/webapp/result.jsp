<%@ page language="java" contentType="text/html; charset=UTF-8"
    pageEncoding="UTF-8" import="java.util.*" %>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<title>HBase Demo</title>
</head>
<body>
<h4>
<%
out.println(request.getAttribute("message"));
%>
</h4>
<h1>HBase Access</h1>
<form method="get" action="HBaseServlet">
<%
out.println("HBase Table<input type=\"text\" name=\"hbaseTableName\" value=\"" + request.getParameter("hbaseTableName") + "\">");
out.println("Column family<input type=\"text\" name=\"columnFamily\" value=\"" + request.getParameter("columnFamily") + "\">");
out.println("Columns (comma separated)<input type=\"text\" name=\"columns\" value=\"" + request.getParameter("columns") + "\">");
%>
<input type="submit" name="action" value="Query">
<input type="submit" name="action" value="Create">
<p>
<%
if(request.getParameter("action").equals("Query")) {
  out.println("<center>" + ((ArrayList<String[]>)request.getAttribute("result")).size() + " results found</center>");
  out.println("<table border='1'>");

  ArrayList<String[]> result = (ArrayList<String[]>) request.getAttribute("result");
  for(String[] columns: result) {
    out.println("<tr>");
    for(String column: columns) {
	  out.println("<td>" + column + "</td>");
    }
    out.println("</tr>");
  }
  out.println("</table>");
}
%>
</form>

</body>
</html>