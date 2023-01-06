#!/usr/bin/env python
# coding=utf-8
import requests
import os
import json
import threading
import datetime

"""统计的时间区间-开始日期"""
git_root_url = "https://git.example.com"
"""访问Token"""
git_token = "your token"
"""统计结果的存储目录"""
export_path = "/tmp"
"""统计的时间区间-开始日期"""
t_from = "2018-07-23"
"""统计的时间区间-结束日期"""
t_end = "2018-07-30"
utc_8 = datetime.timezone(datetime.timedelta(hours=8))
"""统计的时间区间-开始日期，datetime对象"""
date_from = datetime.datetime.strptime(t_from, '%Y-%m-%d')
date_from = date_from.replace(tzinfo=utc_8)
"""统计的时间区间-结束日期，datetime对象"""
date_end = datetime.datetime.strptime(t_end, '%Y-%m-%d')
date_end = date_end.replace(tzinfo=utc_8)
"""一个线程锁"""
lock = threading.RLock()
countLock = threading.Lock()

user_unknown = {}
user_email_alias_mapping = {}
user_email_name_mapping = {}
total_project_commit_count = 0
projects_commit_count = {}
project_count = 0

class GitlabApiCountTrueLeTrue:
    """
    Worker类
    """
    """
    所有commit的集合，用于去重。
    这里的重复，可能是代码merge造成的
    """
    total_commit_map = {}

    """
    最终的数据集合
    """
    totalMap = {}

    def get_projects(self):
        """
        获取所有仓库，并生成报告
        :return:
        """
        threads = []
        # 获取服务器上的所有仓库，每个仓库新建一个线程  1,3表示1-2页 这里per_page最大100，指定1000也没用
        for i in range(1, 3):
            # 线上gitlab可用，问题是没有全部显示
            url = '%s/api/v4/projects' \
                  '?private_token=%s&per_page=1000&page=%d&order_by=last_activity_at' % (
                      git_root_url, git_token, i)
            response = requests.get(url)  # 请求url，传入header，ssl认证为false
            projects = response.json()  # 显示json字符串
            with countLock:
                global project_count
                project_count += len(projects)
            for project in projects:
                default_branch = project['default_branch']
                last_active_time = project['last_activity_at']
                if default_branch is None:
                    continue
                days = date_from - datetime.datetime.strptime(last_active_time, '%Y-%m-%dT%H:%M:%S.%f%z')
                # 如果project的最后更新时间比起始时间小，则continue
                if days.days > 1:
                    continue
                project_info = ProjectInfo()
                project_info.project_id = project['id']
                project_info.name = project['name']
                project_info.project_desc = project['description']
                project_info.project_url = project['web_url']
                project_info.path = project['path']
                project_info.default_branch = default_branch
                # 构件好线程
                t = threading.Thread(target=self.get_branches, args=(project['id'], project_info))
                threads.append(t)
        # 所有线程逐一开始
        for t in threads:
            t.start()
        # 等待所有线程结束
        for t in threads:
            t.join()
        final_commit_map = {}
        for key, project in self.totalMap.items():
            for author_email, detail in project.commit_map.items():
                exist_detail = final_commit_map.get(detail.author_email)
                if exist_detail is None:
                    final_commit_map[detail.author_email] = detail
                else:
                    exist_detail.total += detail.total
                    exist_detail.additions += detail.additions
                    exist_detail.deletions += detail.deletions
                    final_commit_map[detail.author_email] = exist_detail
        write_to_csv("%s/GitStatic_%s/%s_%s.csv" % (export_path, t_from, 'total', t_from), final_commit_map,
                     "extra")
        return

    def get_branches(self, project_id, project_info):
        """
        获取仓库的所有Branch，并汇总commit到一个map梨
        :param project_id:
        :param project_info:
        :return:
        """
        print("进入线程：%d,项目id%d,%s" % (threading.get_ident(), project_id, project_info.project_url))
        # 线上gitlab可用，问题是没有全部显示
        url = '%s/api/v4/projects/%s/repository/branches?private_token=%s' % (git_root_url, project_id, git_token)

        print("start get branch list %d,url=%s" % (project_id, url))

        response = requests.get(url)  # 请求url，传入header，ssl认证为false
        branches = response.json()  # 显示json字符串
        if not branches:
            return
        # branch的map，key为branch名称，value为按照提交者email汇总的，key为email的子map集合
        branch_map = {}
        # 主动获取master分支的提交 master不一定是默认分支 主动获取干什么呢？这里取消了
        # detail_map = self.get_commits(project_id, project_info.project_url, project_info.default_branch)
        # print("get commits finish project_id=%d branch master" % project_id)

        # if detail_map:
        #     branch_map[project_info.default_branch] = detail_map
        for branch in branches:
            branch_name = branch['name']
            if branch_name is None:
                continue
            # 如果仓库已经被Merge了，则不再处理
            if branch['merged']:
                continue
            detail_map = self.get_commits(project_id, project_info.project_url, branch_name)
            if not detail_map:
                continue
            # 将结果放到map里
            branch_map[branch_name] = detail_map
            print("get commits finish project_id=%d branch %s" % (project_id, branch_name))

        print("all branch commits finish %d " % project_id)

        final_commit_map = {}
        # 遍历branch map，并按照提交者email进行汇总
        for key, value_map in branch_map.items():
            for author_email, detail in value_map.items():
                exist_detail = final_commit_map.get(detail.author_email)
                if exist_detail is None:
                    final_commit_map[detail.author_email] = detail
                else:
                    exist_detail.total += detail.total
                    exist_detail.additions += detail.additions
                    exist_detail.deletions += detail.deletions
                    final_commit_map[detail.author_email] = exist_detail

        if not final_commit_map:
            return

        project_info.commit_map = final_commit_map
        # 加锁
        lock.acquire()
        # 此对象会被各个线程操作
        self.totalMap[project_info.project_id] = project_info
        # 释放锁
        lock.release()
        # 汇总完毕后，将结果写入到projectID+日期的csv文件里
        write_to_csv(
            "%s/GitStatic_%s/project/%s_%d.csv" % (export_path, t_from, project_info.path, project_info.project_id),
            final_commit_map, project_info.project_url)

    def get_commits(self, project_id, project_url, branch_name):
        """
        获取指定仓库，指定分支的所有commits，然后遍历每一个commit获得单个branch的统计信息
        :param project_id:
        :param project_url:
        :param branch_name:
        :return:
        """
        since_date = date_from.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        until_date = date_end.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        # 这里per_page大一点，不加不够
        url = '%s/api/v4/projects/%s/repository/commits?page=1&per_page=10000&ref_name=%s&since=%s&until=%s&private_token=%s' % (
            git_root_url, project_id, branch_name, since_date, until_date, git_token)
        response = requests.get(url)  # 请求url，传入header，ssl认证为false
        commits = response.json()  # 显示json字符串
        if not commits:
            return
        print('start get_commits,projectID=%d,branch=%s,url=%s' % (project_id, branch_name, url))

        details = {}
        project_commit_count = len(commits)
        with countLock:
            global total_project_commit_count 
            total_project_commit_count += project_commit_count
            if project_url in projects_commit_count:
                projects_commit_count[project_url] += project_commit_count
            else:
                projects_commit_count[project_url] = project_commit_count
        for commit in commits:
            commit_id = commit['id']
            if commit_id is None:
                continue
            # 在这里进行commit去重判断
            if self.total_commit_map.get(commit_id) is None:
                self.total_commit_map[commit_id] = commit_id
            else:
                continue
            # 这里开始获取单次提交详情
            detail = get_commit_detail(project_id, commit_id)
            if detail is None:
                continue
            if detail.total > 5000:
                # 单次提交大于5000行的代码，可能是脚手架之类生成的代码，不做处理
                continue
            # 这里和主流程无关，是用来处理commit记录里的提交者，账号不规范的问题
            if detail.author_email in user_unknown:
                print("email %s projectid= %d,branchname,%s,url=%s" % (
                    detail.author_email, project_id, branch_name, project_url))

            # 根据email纬度，统计提交数据
            exist_detail = details.get(detail.author_email)
            if exist_detail is None:
                details[detail.author_email] = detail
            else:
                exist_detail.total += detail.total
                exist_detail.additions += detail.additions
                exist_detail.deletions += detail.deletions
                details[detail.author_email] = exist_detail
        return details


def get_commit_detail(project_id, commit_id):
    """
    获取单个commit的信息
    :param project_id: 工程ID
    :param commit_id: commit的id
    :return: 返回#CommitDetails对象
    """
    url = '%s/api/v4/projects/%s/repository/commits/%s?private_token=%s' \
          % (git_root_url, project_id, commit_id, git_token)
    r1 = requests.get(url)  # 请求url，传入header，ssl认证为false
    r2 = r1.json()  # 显示json字符串
    # print(json.dumps(r2, ensure_ascii=False))
    author_name = r2['author_name']
    author_email = r2['author_email']

    stats = r2['stats']
    if 'Merge branch' in r2['title']:
        return
    if stats is None:
        return
    temp_mail = user_email_alias_mapping.get(author_email)
    if temp_mail is not None:
        author_email = temp_mail
    temp_name = user_email_name_mapping.get(author_email)
    if temp_name is not None:
        author_name = temp_name
    additions = stats['additions']
    deletions = stats['deletions']
    total = stats['total']
    # details = {'additions': additions, 'deletions': deletions, 'total': total, 'author_email': author_email,
    #            'author_name': author_name}
    details = CommitDetails()
    details.additions = additions
    details.deletions = deletions
    details.total = total
    details.author_email = author_email

    details.author_name = author_name
    return details


def make_dir_safe(file_path):
    """
    工具方法：写文件时，如果关联的目录不存在，则进行创建
    :param file_path:文件路径或者文件夹路径
    :return:
    """
    if file_path.endswith("/"):
        if not os.path.exists(file_path):
            os.makedirs(file_path)
    else:
        folder_path = file_path[0:file_path.rfind('/') + 1]
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)


def write_to_csv(file_path, final_commit_map, extra):
    """
    工具方法：将结果写入csv，从#final_commit_map参数解析业务数据
    :param file_path:文件路径
    :param final_commit_map:提交参数
    :param extra:额外数据列
    :return:
    """
    make_dir_safe(file_path)
    with open(file_path, 'w') as out:
        title = '%s,%s,%s,%s,%s,%s' % (
            "提交人邮箱", "提交人姓名", "总行数", "增加行数", "删除行数", extra)
        out.write(title + "\n")
        # print(title)
        for key, value in final_commit_map.items():
            var = '%s,%s,%s,%s,%s' % (
                value.author_email, value.author_name, value.total, value.additions, value.deletions)
            out.write(var + '\n')
            # print(var)
        out.close()


class CommitDetails(json.JSONEncoder):
    """
    提交信息的结构体
    """
    author_name = None
    author_email = None
    additions = 0
    deletions = 0
    total = 0


class ProjectInfo(json.JSONEncoder):
    """
    工程信息的结构体
    """
    project_id = None
    project_desc = None
    project_url = None
    path = None
    name = None
    commit_map = None
    default_branch = None


if __name__ == '__main__':
    gitlab4 = GitlabApiCountTrueLeTrue()
    gitlab4.get_projects()
    print("项目总提交数：%d" % total_project_commit_count)
    for key, value in projects_commit_count.items():
        print("项目id=%s,提交数=%d" % (key, value))