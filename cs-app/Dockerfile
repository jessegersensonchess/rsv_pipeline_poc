# ---- build stage ----
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build

# Copy the entire project folder into the image
WORKDIR /src
# If your project folder is named differently, change both occurrences of `cs-app`

# Run restore & publish from within the project directory
RUN dotnet new console -n CsvLoaderCs
RUN cd CsvLoaderCs && \
dotnet add package Npgsql && \
dotnet add package Microsoft.Data.SqlClient 
# replace Program.cs with the code below
COPY Program.cs /src/CsvLoaderCs/
WORKDIR /src/CsvLoaderCs
RUN dotnet build
##RUN dotnet restore
RUN dotnet publish -c Release -o /app/publish

# ---- runtime stage ----
FROM mcr.microsoft.com/dotnet/runtime:8.0 AS runtime
WORKDIR /app
COPY --from=build /app/publish ./
ENV TZ=Europe/Prague
ENTRYPOINT ["dotnet", "/app/CsvLoaderCs.dll"]

