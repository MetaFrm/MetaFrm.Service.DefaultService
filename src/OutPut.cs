namespace MetaFrm.Service
{
    internal class OutPut
    {
        public string? SourceTableName { get; set; }
        public string? SourceParameterName { get; set; }
        public string? TargetTableName { get; set; }
        public string? TargetParameterName { get; set; }
        public object? Value { get; set; }
    }
}