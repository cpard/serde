#![feature(test)]

use chrono::{DateTime, Utc};
use serde_json::{Error, Value};
use std::fs::File;
use std::io;
use std::io::{BufRead, BufReader, Seek};
use std::sync::Arc;
use arrow_schema::Schema;
use parquet::errors::ParquetError;
use serde::{Serialize, Deserialize};
use arrow_tools::seekable_reader::*;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use arrow::json::RawReaderBuilder;
use arrow::record_batch::RecordBatchReader;
extern crate test;

    #[derive(Deserialize, Debug)]
    pub struct Line {
        #[serde(with = "id_format")]
        id: i64,
        #[serde(rename = "type")]
        _type: Option<String>,
        actor: Actor,
        repo: LeanRepo,
        payload: Payload,
        public: Option<bool>,
        //#[serde(with = "my_date_format")]
        //created_at: Option<DateTime<Utc>>,
        created_at: Option<String>,
        org: Option<Org>,
    }

#[derive(Deserialize, Debug)]
pub struct LeanRepo {
    id: i64,
    name: Option<String>,
    url: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct Payload {
    before: Option<String>,
    commits: Option<Vec<Commit>>,
    distinct_size: Option<i64>,
    head: Option<String>,
    push_id: Option<i64>,
    #[serde(rename = "ref")]
    _ref: Option<String>,
    size: Option<i64>,
    action: Option<String>,
    number: Option<i64>,
    pull_request: Option<PullRequest>,
    description: Option<String>,
    master_branch: Option<String>,
    pusher_type: Option<String>,
    ref_type: Option<String>,
    forkee: Option<Forkee>,
    issue: Option<Issue>,
    comment: Option<Comment>,
    review: Option<Review>,
    member: Option<Actor>,
    release: Option<Release>,
    pages: Option<Vec<Page>>,
}

#[derive(Deserialize, Debug)]
pub struct Review {
    _links: Option<Link>,
    author_association: Option<String>,
    commit_id: Option<String>,
    html_url: Option<String>,
    id: i64,
    node_id: Option<String>,
    pull_request_url: Option<String>,
    state: Option<String>,
    //#[serde(with = "my_date_format")]
    //submitted_at: Option<DateTime<Utc>>,
    submitted_at: Option<String>,
    user: Option<Actor>,
    body: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct Reaction {
    #[serde(rename = "+1")]
    plus_one: Option<i64>,
    #[serde(rename = "-1")]
    minus_one: Option<i64>,
    confused: Option<i64>,
    eyes: Option<i64>,
    heart: Option<i64>,
    hooray: Option<i64>,
    laugh: Option<i64>,
    rocket: Option<i64>,
    total_count: Option<i64>,
    url: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct Comment {
    url: Option<String>,
    html_url: Option<String>,
    issue_url: Option<String>,
    id: i64,
    node_id: Option<String>,
    user: Option<Actor>,
    //#[serde(with = "my_date_format")]
    //created_at: Option<DateTime<Utc>>,
    created_at: Option<String>,
    //#[serde(with = "my_date_format")]
    //updated_at: Option<DateTime<Utc>>,
    updated_at: Option<String>,
    author_association: Option<String>,
    body: Option<String>,
    performed_via_github_app: Option<PerformedViaGithubApp>,
    reactions: Option<Reaction>,
    commit_id: Option<String>,
    _links: Option<Link>,
    diff_hunk: Option<String>,
    in_reply_to_id: Option<i64>,
    line: Option<i64>,
    original_commit_id: Option<String>,
    original_line: Option<i64>,
    original_position: Option<i64>,
    path: Option<String>,
    position: Option<i64>,
    pull_request_review_id: Option<i64>,
    pull_request_url: Option<String>,
    side: Option<String>,
    original_start_line: Option<i64>,
    start_line: Option<i64>,
    start_side: Option<String>,
    performed_via_github_app_id: Option<i64>,
}

#[derive(Deserialize, Debug)]
pub struct PerformedViaGithubApp {
    id: i64,
    slug: Option<String>,
    node_id: Option<String>,
    owner: Option<Actor>,
    name: Option<String>,
    description: Option<String>,
    external_url: Option<String>,
    html_url: Option<String>,
    created_at: Option<String>,
    updated_at: Option<String>,
    permissions: Option<Permission>,
    events: Option<Vec<String>>,
    installations_count: Option<i64>,
}

#[derive(Deserialize, Debug)]
pub struct Permission {
    checks: Option<String>,
    contents: Option<String>,
    deployments: Option<String>,
    issues: Option<String>,
    metadata: Option<String>,
    pull_requests: Option<String>,
    statuses: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct Asset {
    url: Option<String>,
    id: i64,
    node_id: Option<String>,
    name: Option<String>,
    label: Option<String>,
    uploader: Option<Actor>,
    content_type: Option<String>,
    state: Option<String>,
    size: Option<i64>,
    download_count: Option<i64>,
    //#[serde(with = "my_date_format")]
    //created_at: Option<DateTime<Utc>>,
    created_at: Option<String>,
    //#[serde(with = "my_date_format")]
    //updated_at: Option<DateTime<Utc>>,
    updated_at: Option<String>,
    browser_download_url: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct Release {
    url: Option<String>,
    assets_url: Option<String>,
    upload_url: Option<String>,
    html_url: Option<String>,
    id: i64,
    assets: Option<Vec<Asset>>,
    author: Option<Actor>,
    body: Option<String>,
    //#[serde(with = "my_date_format")]
    //created_at: Option<DateTime<Utc>>,
    created_at: Option<String>,
    draft: Option<bool>,
    is_short_description_html_truncated: Option<bool>,
    node_id: Option<String>,
    prerelease: Option<bool>,
    //#[serde(with = "my_date_format")]
    //published_at: Option<DateTime<Utc>>,
    published_at: Option<String>,
    short_description: Option<String>,
    tag_name: Option<String>,
    tarball_url: Option<String>,
    target_commitish: Option<String>,
    zipball_url: Option<String>,
    name: Option<String>,
    mentions: Option<Vec<Mention>>,
    mentions_count: Option<i64>,
    discussion_url: Option<String>,
}
#[derive(Deserialize, Debug)]
pub struct Mention {
    avatar_url: Option<String>,
    avatar_user_actor: Option<bool>,
    login: Option<String>,
    profile_name: Option<String>,
    profile_url: Option<String>,
}
#[derive(Deserialize, Debug)]
pub struct Page {
    page_name: Option<String>,
    title: Option<String>,
    summary: Option<String>,
    action: Option<String>,
    sha: Option<String>,
    html_url: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct Issue {
    url: Option<String>,
    repository_url: Option<String>,
    labels_url: Option<String>,
    comments_url: Option<String>,
    events_url: Option<String>,
    html_url: Option<String>,
    timeline_url: Option<String>,
    id: i64,
    node_id: Option<String>,
    number: Option<i64>,
    title: Option<String>,
    user: Option<Actor>,
    labels: Option<Vec<Label>>,
    state: Option<String>,
    locked: Option<bool>,
    assignee: Option<Actor>,
    assignees: Option<Vec<Actor>>,
    milestone: Option<Milestone>,
    comments: Option<i64>,
    //#[serde(with = "my_date_format")]
    //created_at: Option<DateTime<Utc>>,
    created_at: Option<String>,
    //#[serde(with = "my_date_format")]
    //updated_at: Option<DateTime<Utc>>,
    updated_at: Option<String>,
    //#[serde(with = "my_date_format")]
    //closed_at: Option<DateTime<Utc>>,
    closed_at: Option<String>,
    author_association: Option<String>,
    active_lock_reason: Option<String>,
    body: Option<String>,
    performed_via_github_app: Option<String>,
    reactions: Option<Reaction>,
    draft: Option<bool>,
    pull_request: Option<PullRequestItem>,
    state_reason: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct PullRequestItem {
    diff_url: Option<String>,
    html_url: Option<String>,
    patch_url: Option<String>,
    url: Option<String>,
    //#[serde(with = "my_date_format")]
    //merged_at: Option<DateTime<Utc>>,
    merged_at: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct Forkee {
    allow_forking: Option<bool>,
    archive_url: Option<String>,
    archived: Option<bool>,
    assignees_url: Option<String>,
    blobs_url: Option<String>,
    branches_url: Option<String>,
    clone_url: Option<String>,
    collaborators_url: Option<String>,
    comments_url: Option<String>,
    commits_url: Option<String>,
    compare_url: Option<String>,
    contents_url: Option<String>,
    contributors_url: Option<String>,
    //#[serde(with = "my_date_format")]
    //created_at: Option<DateTime<Utc>>,
    created_at: Option<String>,
    default_branch: Option<String>,
    deployments_url: Option<String>,
    description: Option<String>,
    disabled: Option<bool>,
    downloads_url: Option<String>,
    events_url: Option<String>,
    fork: Option<bool>,
    forks: Option<i64>,
    forks_count: Option<i64>,
    forks_url: Option<String>,
    full_name: Option<String>,
    git_commits_url: Option<String>,
    git_refs_url: Option<String>,
    git_tags_url: Option<String>,
    git_url: Option<String>,
    has_discussions: Option<bool>,
    has_downloads: Option<bool>,
    has_issues: Option<bool>,
    has_pages: Option<bool>,
    has_projects: Option<bool>,
    has_wiki: Option<bool>,
    hooks_url: Option<String>,
    html_url: Option<String>,
    id: i64,
    is_template: Option<bool>,
    issue_comment_url: Option<String>,
    issue_events_url: Option<String>,
    issues_url: Option<String>,
    keys_url: Option<String>,
    labels_url: Option<String>,
    languages_url: Option<String>,
    license: Option<License>,
    merges_url: Option<String>,
    milestones_url: Option<String>,
    name: Option<String>,
    node_id: Option<String>,
    notifications_url: Option<String>,
    open_issues: Option<i64>,
    open_issues_count: Option<i64>,
    owner: Option<Actor>,
    private: Option<bool>,
    public: Option<bool>,
    pulls_url: Option<String>,
    //#[serde(with = "my_date_format")]
    //pushed_at: Option<DateTime<Utc>>,
    pushed_at: Option<String>,
    releases_url: Option<String>,
    size: Option<i64>,
    ssh_url: Option<String>,
    stargazers_count: Option<i64>,
    stargazers_url: Option<String>,
    statuses_url: Option<String>,
    subscribers_url: Option<String>,
    subscription_url: Option<String>,
    svn_url: Option<String>,
    tags_url: Option<String>,
    teams_url: Option<String>,
    topics: Option<Vec<String>>,
    trees_url: Option<String>,
    //#[serde(with = "my_date_format")]
    //updated_at: Option<DateTime<Utc>>,
    updated_at: Option<String>,
    url: Option<String>,
    visibility: Option<String>,
    watchers: Option<i64>,
    watchers_count: Option<i64>,
    web_commit_signoff_required: Option<bool>,
    homepage: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct License {
    key: Option<String>,
    name: Option<String>,
    node_id: Option<String>,
    spdx_id: Option<String>,
    url: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct PullRequest{
    _links: Option<Link>,
    additions: Option<i64>,
    assignees: Option<Vec<Actor>>,
    author_association: Option<String>,
    base: Option<Base>,
    body: Option<String>,
    changed_files: Option<i64>,
    comments: Option<i64>,
    comments_url: Option<String>,
    commits: Option<i64>,
    commits_url: Option<String>,
    //#[serde(with = "my_date_format")]
   // created_at: Option<DateTime<Utc>>,
    created_at: Option<String>,
    deletions: Option<i64>,
    diff_url: Option<String>,
    draft: Option<bool>,
    head: Option<Head>,
    html_url: Option<String>,
    id: i64,
    issue_url: Option<String>,
    labels: Option<Vec<Label>>,
    locked: Option<bool>,
    maintainer_can_modify: Option<bool>,
    mergeable_state: Option<String>,
    merged: Option<bool>,
    node_id: Option<String>,
    number: Option<i64>,
    patch_url: Option<String>,
    requested_reviewers: Option<Vec<Actor>>,
    requested_teams: Option<Vec<Team>>,
    review_comment_url: Option<String>,
    review_comments: Option<i64>,
    review_comments_url: Option<String>,
    state: Option<String>,
    statuses_url: Option<String>,
    title: Option<String>,
    //#[serde(with = "my_date_format")]
    //updated_at: Option<DateTime<Utc>>,
    updated_at: Option<String>,
    url: Option<String>,
    user: Option<Actor>,
    merge_commit_sha: Option<String>,
    //#[serde(with = "my_date_format")]
    closed_at: Option<String>,
    //closed_at: Option<DateTime<Utc>>,
    //#[serde(with = "my_date_format")]
    //merged_at: Option<DateTime<Utc>>,
    merged_at: Option<String>,
    merged_by: Option<Actor>,
    mergeable: Option<bool>,
    rebaseable: Option<bool>,
    assignee: Option<Actor>,
    auto_merge: Option<AutoMerge>,
    milestone: Option<Milestone>,
}

#[derive(Deserialize, Debug)]
pub struct Base {
    label: Option<String>,
    #[serde(rename = "ref")]
    _ref: Option<String>,
    repo: Option<Repo>,
    sha: Option<String>,
    user: Option<Actor>,
}

#[derive(Deserialize, Debug)]
pub struct Link {
    comments: Option<Href>,
    commits: Option<Href>,
    html: Option<Href>,
    issue: Option<Href>,
    review_comment: Option<Href>,
    review_comments: Option<Href>,
    pull_request: Option<Href>,
    self_: Option<Href>,
    statuses: Option<Href>,
}

#[derive(Deserialize, Debug)]
pub struct Href {
    href: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct Milestone {
    closed_issues: Option<i64>,
    //#[serde(with = "my_date_format")]
    //created_at: Option<DateTime<Utc>>,
    created_at: Option<String>,
    creator: Option<Actor>,
    description: Option<String>,
    html_url: Option<String>,
    id: i64,
    labels_url: Option<String>,
    node_id: Option<String>,
    number: Option<i64>,
    open_issues: Option<i64>,
    state: Option<String>,
    title: Option<String>,
    //#[serde(with = "my_date_format")]
    //updated_at: Option<DateTime<Utc>>,
    updated_at: Option<String>,
    url: Option<String>,
    //#[serde(with = "my_date_format")]
    //due_on: Option<DateTime<Utc>>,
    due_on: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct AutoMerge {
    commit_message: Option<String>,
    commit_title: Option<String>,
    enabled_by: Option<Actor>,
    merge_method: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct Team {
    id: i64,
    description: Option<String>,
    html_url: Option<String>,
    members_url: Option<String>,
    name: Option<String>,
    node_id: Option<String>,
    permission: Option<String>,
    privacy: Option<String>,
    repositories_url: Option<String>,
    slug: Option<String>,
    url: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct Label {
    color: Option<String>,
    default: Option<bool>,
    description: Option<String>,
    id: i64,
    name: Option<String>,
    node_id: Option<String>,
    url: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct Head {
    label: Option<String>,
    #[serde(rename = "ref")]
    _ref: Option<String>,
    repo: Option<Repo>,
    sha: Option<String>,
    user: Option<Actor>,
}

#[derive(Deserialize, Debug)]
pub struct Commit {
    author: Option<Author>,
    distinct: Option<bool>,
    message: Option<String>,
    sha: Option<String>,
    url: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct Author {
    email: Option<String>,
    name: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct Org {
    id: i64,
    login: Option<String>,
    gravatar_id: Option<String>,
    url: Option<String>,
    avatar_url: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct Repo {
    id: i64,
    name: Option<String>,
    url: Option<String>,
    allow_forking: Option<bool>,
    archive_url: Option<String>,
    archived: Option<bool>,
    assignees_url: Option<String>,
    blobs_url: Option<String>,
    branches_url: Option<String>,
    clone_url: Option<String>,
    collaborators_url: Option<String>,
    comments_url: Option<String>,
    commits_url: Option<String>,
    compare_url: Option<String>,
    contents_url: Option<String>,
    contributors_url: Option<String>,
    //#[serde(with = "my_date_format")]
    //created_at: Option<DateTime<Utc>>,
    created_at: Option<String>,
    default_branch: Option<String>,
    deployments_url: Option<String>,
    disabled: Option<bool>,
    downloads_url: Option<String>,
    events_url: Option<String>,
    fork: Option<bool>,
    forks: Option<i64>,
    forks_count: Option<i64>,
    forks_url: Option<String>,
    full_name: Option<String>,
    git_commits_url: Option<String>,
    git_refs_url: Option<String>,
    git_tags_url: Option<String>,
    git_url: Option<String>,
    has_discussions: Option<bool>,
    has_downloads: Option<bool>,
    has_issues: Option<bool>,
    has_pages: Option<bool>,
    has_projects: Option<bool>,
    has_wiki: Option<bool>,
    hooks_url: Option<String>,
    html_url: Option<String>,
    is_template: Option<bool>,
    issue_comment_url: Option<String>,
    issue_events_url: Option<String>,
    issues_url: Option<String>,
    keys_url: Option<String>,
    labels_url: Option<String>,
    language: Option<String>,
    languages_url: Option<String>,
    merges_url: Option<String>,
    milestones_url: Option<String>,
    node_id: Option<String>,
    notifications_url: Option<String>,
    open_issues: Option<i64>,
    open_issues_count: Option<i64>,
    owner: Option<Actor>,
    private: Option<bool>,
    pulls_url: Option<String>,
   // #[serde(with = "my_date_format")]
    //pushed_at: Option<DateTime<Utc>>,
    pushed_at: Option<String>,
    releases_url: Option<String>,
    size: Option<i64>,
    ssh_url: Option<String>,
    stargazers_count: Option<i64>,
    stargazers_url: Option<String>,
    statuses_url: Option<String>,
    subscribers_url: Option<String>,
    subscription_url: Option<String>,
    svn_url: Option<String>,
    tags_url: Option<String>,
    teams_url: Option<String>,
    topics: Option<Vec<String>>,
    trees_url: Option<String>,
    //#[serde(with = "my_date_format")]
    //updated_at: Option<DateTime<Utc>>,
    updated_at: Option<String>,
    visibility: Option<String>,
    watchers: Option<i64>,
    watchers_count: Option<i64>,
    web_commit_signoff_required: Option<bool>,
    description: Option<String>,
    license: Option<License>,
    homepage: Option<String>,
    mirror_url: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct Actor {
    id: i64,
    login: Option<String>,
    display_login: Option<String>,
    gravatar_id: Option<String>,
    url: Option<String>,
    avatar_url: Option<String>,
    events_url: Option<String>,
    followers_url: Option<String>,
    following_url: Option<String>,
    gists_url: Option<String>,
    html_url: Option<String>,
    node_id: Option<String>,
    organizations_url: Option<String>,
    received_events_url: Option<String>,
    repos_url: Option<String>,
    site_admin: Option<bool>,
    starred_url: Option<String>,
    subscriptions_url: Option<String>,
    #[serde(rename = "type")]
    _type: Option<String>,
}

    pub fn read_file(filename: String) -> io::Lines<BufReader<File>>{
        let file = File::open(filename).unwrap();
        return io::BufReader::new(file).lines();
    }

    pub fn to_struct(line: &str) -> Line {
        let line: Line = serde_json::from_str(line).unwrap();
        return line;
    }

    mod id_format {
        use serde::{self, Deserialize, Serializer, Deserializer};
        pub fn serialize<S>(
            id: i64,
            serializer: S,
        ) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
        {
            let s = id.to_string();
            serializer.serialize_str(&s)
        }

        pub fn deserialize<'de, D>(
            deserializer: D,
        ) -> Result<i64, D::Error>
            where
                D: Deserializer<'de>,
        {
            let s = String::deserialize(deserializer)?;
            s.parse::<i64>().map_err(serde::de::Error::custom)
        }
    }

    mod my_date_format {
        use chrono::{DateTime, Utc, TimeZone};
        use serde::{self, Deserialize, Serializer, Deserializer};

        const FORMAT: &'static str = "%Y-%m-%dT%H:%M:%SZ";

        // The signature of a serialize_with function must follow the pattern:
        //
        //    fn serialize<S>(&T, S) -> Result<S::Ok, S::Error>
        //    where
        //        S: Serializer
        //
        // although it may also be generic over the input types T.
        pub fn serialize<S>(
            date: &DateTime<Utc>,
            serializer: S,
        ) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
        {
            let s = format!("{}", date.format(FORMAT));
            serializer.serialize_str(&s)
        }

        // The signature of a deserialize_with function must follow the pattern:
        //
        //    fn deserialize<'de, D>(D) -> Result<T, D::Error>
        //    where
        //        D: Deserializer<'de>
        //
        // although it may also be generic over the output types T.
        pub fn deserialize<'de, D>(
            deserializer: D,
        ) -> Result<Option<DateTime<Utc>>, D::Error>
            where
                D: Deserializer<'de>,
        {
            let s = String::deserialize(deserializer)?;
            let res: Result<DateTime<Utc>, D::Error> = Utc.datetime_from_str(&s, FORMAT).map_err(serde::de::Error::custom);
            return match res {
                Ok(date) => Ok(Some(date)),
                Err(_) => Ok(None),
            };
        }
    }


pub fn get_bufreader(file: String) -> Result<BufReader<Box<dyn SeekRead>>, ParquetError> {
    let mut file = File::open(file).unwrap();

    let input: Box<dyn SeekRead> = if file.rewind().is_ok() {
        Box::new(file)
    } else {
        Box::new(SeekableReader::from_unbuffered_reader(
            file,
            Some(10000),
        ))
    };

    let buf_reader = BufReader::new(input);
    return Ok(buf_reader);
}

pub fn infer_schema(file: String) -> Result<Schema, ParquetError> {
    let mut file = File::open(file).unwrap();

    let input: Box<dyn SeekRead> = if file.rewind().is_ok() {
        Box::new(file)
    } else {
        Box::new(SeekableReader::from_unbuffered_reader(
            file,
            Some(161519),
        ))
    };

    let mut buf_reader = BufReader::new(input);

    let schema = arrow::json::reader::infer_json_schema_from_seekable(
        &mut buf_reader, Some(161519))
        .map_err(|err| ParquetError::General(format!("Error inferring schema: {err}")));
    return schema;
}


pub fn load_schema(file: &str) -> Result<Schema, ParquetError>  {
    let schema_file = File::open(file).map_err(|error| {
        ParquetError::General(format!(
            "Error opening schema file: {file:?}, message: {error}"
        ))
    })?;
    let schema: Result<arrow::datatypes::Schema, serde_json::Error> =
        serde_json::from_reader(schema_file);
    schema.map_err(|error| ParquetError::General(format!("Error reading schema json: {error}")))
}

pub fn write_parquet(schema: Schema,
                     buf_reader: BufReader<Box<dyn SeekRead>>,
                     output: File) -> Result<(), ParquetError> {
    let schema_ref = Arc::new(schema);
    let builder = RawReaderBuilder::new(schema_ref);
    let reader = builder.build(buf_reader).unwrap();

    let mut props = WriterProperties::builder().set_dictionary_enabled(true);
    props = props.set_statistics_enabled(EnabledStatistics::Page);
    props = props.set_compression(Compression::ZSTD(ZstdLevel::default()));
    props = props.set_encoding(Encoding::RLE_DICTIONARY);

    let mut writer = ArrowWriter::try_new(output,
                                          reader.schema(),
                                          Some(props.build())).unwrap();
    for batch in reader {
        writer.write(&batch?)?;
    }

    writer.close().map(|_| ())
}

fn main() {

    let schema = load_schema("../../data/out.schema").unwrap();
    let buf_reader = get_bufreader("../../data/2023-01-01-15.json".to_string()).unwrap();
    let output = File::create("../../data/2023-01-01-15.parquet").unwrap();
    write_parquet(schema, buf_reader, output).unwrap();


    //use std::time::Instant;
    //let now = Instant::now();
    //let schema = infer_schema("../../data/2023-01-01-15.json".to_string());
   // let elapsed = now.elapsed();
   // println!("Elapsed: {:.2?}", elapsed);
    //println!("{:?}", schema);
    /*let lines = read_file("../../data/2023-01-01-15.json".to_string());
    use std::time::Instant;
    let now = Instant::now();
    for line in lines {
        let line = line.unwrap();
        let line: Line = to_struct(&line);
    }
    let elapsed = now.elapsed();
    println!("Elapsed: {:.2?}", elapsed);*/
}
